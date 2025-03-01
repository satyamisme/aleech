from __future__ import annotations
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs, listdir, remove
from aioshutil import move, rmtree
from ast import literal_eval
from asyncio import create_subprocess_exec, sleep, gather, Event, Lock, wait_for
from asyncio.subprocess import PIPE
from natsort import natsorted
from os import path as ospath, walk
from time import time

from bot import config_dict, task_dict, task_dict_lock, queue_dict_lock, non_queued_dl, LOGGER, VID_MODE, FFMPEG_NAME
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec, new_task
from bot.helper.ext_utils.files_utils import get_path_size, clean_target
from bot.helper.ext_utils.links_utils import get_url_name
from bot.helper.ext_utils.media_utils import get_document_type, get_media_info, FFProgress
from bot.helper.ext_utils.task_manager import check_running_tasks
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, update_status_message
from bot.helper.video_utils.extra_selector import ExtraSelect

file_lock = Lock()
zip_lock = Lock()
io_lock = Lock()

async def get_metavideo(video_file):
    async with io_lock:
        try:
            stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_format', '-show_streams', video_file])
            if rcode != 0:
                LOGGER.error(f"ffprobe error for {video_file}: {stderr}")
                return [], {}
            metadata = literal_eval(stdout)
            return metadata.get('streams', []), metadata.get('format', {})
        except Exception as e:
            LOGGER.error(f"Error in get_metavideo: {e}")
            return [], {}

class VidEcxecutor(FFProgress):
    def __init__(self, listener: task.TaskListener, path: str, gid: str, metadata=False):
        self.data = {}
        self.event = Event()
        self.listener = listener
        self.path = path
        self.name = ''
        self.outfile = ''
        self.size = 0
        self._metadata = metadata
        self._up_path = path
        self._gid = gid
        self._start_time = time()
        self._files = []
        self._qual = {'1080p': '1920', '720p': '1280', '540p': '960', '480p': '854', '360p': '640'}
        super().__init__()
        self.is_cancel = False

    async def _cleanup(self):
        """Clean up temporary files, directories, and state for this task."""
        async with file_lock:
            try:
                await gather(*[clean_target(file) for file in self._files if await aiopath.exists(file)])
                self._files.clear()
                await clean_target(ospath.join(self.path, f'input_{self._gid}.txt'))
                for extracted_dir in [d for d in self._files if await aiopath.isdir(d)]:
                    await rmtree(extracted_dir, ignore_errors=True)
                self.data.clear()
                self.event.clear()
                self.is_cancel = True
                LOGGER.info(f"Cleanup completed for {self.mode}")
            except Exception as e:
                LOGGER.error(f"Cleanup error: {e}")

    async def _queue(self, update=False):
        if self._metadata:
            add_to_queue, event = await check_running_tasks(self.listener.mid)
            if add_to_queue:
                LOGGER.info(f'Added to Queue/Download: {self.name}')
                async with task_dict_lock:
                    task_dict[self.listener.mid] = QueueStatus(self.listener, self.size, self._gid, 'dl')
                await self.listener.onDownloadStart()
                if update:
                    await sendStatusMessage(self.listener.message)
                try:
                    await wait_for(event.wait(), timeout=300)
                except TimeoutError:
                    LOGGER.error(f"Queue timeout for {self.name}")
                    await self._cleanup()
                    self.is_cancel = True
                    async with task_dict_lock:
                        if self.listener.mid in task_dict:
                            del task_dict[self.listener.mid]
                    return
            async with queue_dict_lock:
                non_queued_dl.add(self.listener.mid)

    async def _extract_zip(self, zip_path):
        async with zip_lock:
            extract_dir = ospath.join(ospath.dirname(zip_path), f"{ospath.splitext(ospath.basename(zip_path))[0]}_{int(time())}")
            try:
                if not await aiopath.exists(extract_dir):
                    await makedirs(extract_dir, exist_ok=True)
                    cmd = ['7z', 'x', zip_path, f'-o{extract_dir}']
                    stdout, stderr, rcode = await cmd_exec(cmd)
                    if rcode != 0:
                        LOGGER.error(f"Failed to extract {zip_path}: {stderr}")
                        await rmtree(extract_dir, ignore_errors=True)
                        await clean_target(zip_path)
                        return None
                for dirpath, _, files in await sync_to_async(walk, extract_dir):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            return extract_dir
                await rmtree(extract_dir, ignore_errors=True)
                await clean_target(zip_path)
                return None
            except Exception as e:
                LOGGER.error(f"ZIP extraction error: {e}")
                await rmtree(extract_dir, ignore_errors=True)
                await clean_target(zip_path)
                return None

    async def _get_files(self):
        file_list = []
        async with io_lock:
            if self._metadata:
                file_list.append(self.path)
            elif await aiopath.isfile(self.path):
                if (await get_document_type(self.path))[0]:
                    file_list.append(self.path)
                elif self.path.lower().endswith(('.zip', '.rar')):
                    extract_dir = await self._extract_zip(self.path)
                    if extract_dir:
                        self.path = extract_dir  # Update self.path to extracted directory
                        for dirpath, _, files in await sync_to_async(walk, extract_dir):
                            for file in natsorted(files):
                                if (await get_document_type(ospath.join(dirpath, file)))[0]:
                                    file_list.append(ospath.join(dirpath, file))
                        self._files.append(extract_dir)
            else:
                for dirpath, _, files in await sync_to_async(walk, self.path):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            file_list.append(file_path)
                        elif file_path.lower().endswith(('.zip', '.rar')):
                            extract_dir = await self._extract_zip(file_path)
                            if extract_dir:
                                for sub_dirpath, _, sub_files in await sync_to_async(walk, extract_dir):
                                    for sub_file in natsorted(sub_files):
                                        if (await get_document_type(ospath.join(sub_dirpath, sub_file)))[0]:
                                            file_list.append(ospath.join(sub_dirpath, sub_file))
                                self._files.append(extract_dir)
        LOGGER.info(f"Extracted files for {self.mode}: {file_list}")
        return file_list

    async def execute(self):
        self._is_dir = await aiopath.isdir(self.path)
        self.mode, self.name, kwargs = self.listener.vidMode
        LOGGER.info(f"Executing {self.mode} with name: {self.name}")
        if not self._metadata and self.mode in config_dict['DISABLE_MULTI_VIDTOOLS']:
            path = await self._get_files()
            self.path = path[0] if path else self._up_path
        if self._metadata:
            if not self.name:
                self.name = get_url_name(self.path)
            if not self.name.upper().endswith(('MP4', 'MKV')):
                self.name += '.mkv'
            try:
                self.size = int(self._metadata[1]['size'])
            except Exception as e:
                LOGGER.error(f"Invalid metadata: {e}")
                await self._cleanup()
                await self.listener.onDownloadError('Invalid data, check the link!')
                return self._up_path

        try:
            match self.mode:
                case 'vid_vid':
                    result = await self._merge_vids()
                case 'vid_aud':
                    result = await self._merge_auds()
                case 'vid_sub':
                    result = await self._merge_subs(**kwargs)
                case 'trim':
                    result = await self._vid_trimmer(**kwargs)
                case 'watermark':
                    result = await self._vid_marker(**kwargs)
                case 'compress':
                    result = await self._vid_compress(**kwargs)
                case 'subsync':
                    result = await self._subsync(**kwargs)
                case 'rmstream':
                    result = await self._rm_stream()
                case 'extract':
                    result = await self._vid_extract()
                case 'merge_rmaudio':
                    result = await self._merge_and_rmaudio()
                case 'merge_preremove_audio':
                    result = await self._merge_preremove_audio()
                case _:
                    result = await self._vid_convert()
            LOGGER.info(f"{self.mode} completed with result: {result}")
            return result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}", exc_info=True)
            await self._cleanup()
            return self._up_path
        finally:
            if self.is_cancel or not result:
                await self._cleanup()

    @new_task
    async def _start_handler(self, *args):
        LOGGER.info(f"Starting handler for {self.mode} with args: {args}")
        await sleep(0.5)
        await ExtraSelect(self).get_buttons(*args)

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update for {self.mode}: {status}")
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}")

    async def _final_path(self, outfile=''):
        async with file_lock:
            try:
                if self._metadata:
                    self._up_path = outfile or self.outfile
                else:
                    scan_dir = self._up_path if self._is_dir else ospath.split(self._up_path)[0]
                    for dirpath, _, files in await sync_to_async(walk, scan_dir):
                        for file in files:
                            if file.endswith(tuple(self.listener.extensionFilter)):
                                await clean_target(ospath.join(dirpath, file))
                    all_files = [(dirpath, file) for dirpath, _, files in await sync_to_async(walk, scan_dir) for file in files]
                    if len(all_files) == 1:
                        self._up_path = ospath.join(*all_files[0])
                for extracted_dir in self._files:
                    if await aiopath.isdir(extracted_dir):
                        await rmtree(extracted_dir, ignore_errors=True)
                self._files.clear()
                LOGGER.info(f"Final path set to: {self._up_path}")
                return self._up_path
            except Exception as e:
                LOGGER.error(f"Final path cleanup error: {e}")
                await self._cleanup()
                return self._up_path

    async def _name_base_dir(self, path, info: str=None, multi: bool=False):
        async with file_lock:
            base_dir, file_name = ospath.split(path)
            if not self.name or multi:
                if info:
                    if await aiopath.isfile(path):
                        file_name = file_name.rsplit('.', 1)[0]
                    file_name += f'_{info}.mkv'
                self.name = file_name
            if not self.name.upper().endswith(('MP4', 'MKV')):
                self.name += '.mkv'
            LOGGER.info(f"Set name: {self.name} with base_dir: {base_dir}")
            return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        await self._send_status(status)
        LOGGER.info(f"Running FFmpeg command: {' '.join(cmd)}")
        self.listener.suproc = await create_subprocess_exec(*cmd, stderr=PIPE)
        try:
            _, code = await gather(self.progress(status), self.listener.suproc.wait())
            if code == 0:
                if not self.listener.seed:
                    await gather(*[clean_target(file) for file in self._files if await aiopath.exists(file)])
                self._files.clear()
                LOGGER.info(f"FFmpeg command succeeded for {self.mode}")
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancel = True
                await self._cleanup()
                LOGGER.info(f"FFmpeg cancelled for {self.mode}")
            else:
                error_msg = (await self.listener.suproc.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error: {error_msg}")
                self.is_cancel = True
                await self._cleanup()
            return False
        except Exception as e:
            LOGGER.error(f"Run command error: {e}")
            self.is_cancel = True
            await self._cleanup()
            return False

    async def _merge_and_rmaudio(self):
        file_list = await self._get_files()
        if not file_list:
            LOGGER.error("No valid video files found for merging.")
            await self._cleanup()
            return self._up_path
        if len(file_list) == 1:
            self.path = file_list[0]
            result = await self._rm_audio_single()
            if self.is_cancel or not result:
                await self._cleanup()
            return result

        self.size = sum(await gather(*[get_path_size(f) for f in file_list]))
        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', True)

        async with file_lock:
            self.outfile = ospath.join(base_dir, f"temp_merge_{self._gid}.mkv")
            self._files = file_list
            input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
            try:
                async with aiopen(input_file, 'w') as f:
                    await f.write('\n'.join([f"file '{f}'" for f in file_list]))
                cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
                if not await self._run_cmd(cmd, 'direct'):
                    LOGGER.error("Failed to merge files due to incompatible formats or FFmpeg error.")
                    await self._cleanup()
                    return self._up_path
            except Exception as e:
                LOGGER.error(f"Error writing input file or running merge: {e}")
                await self._cleanup()
                return self._up_path
            finally:
                await clean_target(input_file)

        streams, _ = await get_metavideo(self.outfile)
        if not streams:
            LOGGER.error("Merge failed: No streams detected in merged file.")
            await self._cleanup()
            return self._up_path

        self._start_handler(streams)
        await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
        LOGGER.info(f"Event received in _merge_and_rmaudio, proceeding with selections: {self.data.get('streams_to_remove', [])}")

        if self.is_cancel or not self.data:
            LOGGER.warning("Merge and remove audio cancelled or no selections made.")
            await self._cleanup()
            return self.outfile

        streams_to_remove = self.data.get('streams_to_remove', [])
        if not streams_to_remove:
            LOGGER.info("No streams selected to remove, keeping all.")
            result = await self._final_path(self.outfile)
            await self._cleanup()
            return result

        final_outfile = ospath.join(base_dir, self.name)
        cmd = [FFMPEG_NAME, '-i', self.outfile]
        kept_streams = [f'0:{s["index"]}' for s in self.data['streams'].values() if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
        cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
        cmd.extend(('-c', 'copy', final_outfile, '-y'))
        if not await self._run_cmd(cmd, 'direct'):
            LOGGER.error("Failed to remove selected tracks from merged file.")
            await self._cleanup()
            return self.outfile

        await clean_target(self.outfile)
        result = await self._final_path(final_outfile)
        if self.is_cancel or not result:
            await self._cleanup()
        return result

    async def _merge_preremove_audio(self):
        file_list = await self._get_files()
        if not file_list:
            LOGGER.error("No valid video files found for processing.")
            await self._cleanup()
            return self._up_path
        if len(file_list) == 1:
            self.path = file_list[0]
            result = await self._rm_audio_single()
            if self.is_cancel or not result:
                await self._cleanup()
            return result

        stream_data = await gather(*[get_metavideo(f) for f in file_list])
        streams_per_file = {f: streams for f, (streams, _) in zip(file_list, stream_data)}
        self.size = sum(await gather(*[get_path_size(f) for f in file_list]))
        base_dir = await self._name_base_dir(file_list[0], 'Merge-PreRemoveAudio', True)

        temp_files = []
        for file in file_list:
            if not streams_per_file[file]:
                LOGGER.warning(f"No streams found in {file}, skipping.")
                continue
            self._start_handler(streams_per_file[file])
            await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
            LOGGER.info(f"Event received for {file} in _merge_preremove_audio, selections: {self.data.get('streams_to_remove', [])}")
            if self.is_cancel or not self.data:
                await self._cleanup()
                return self._up_path

            outfile = ospath.join(base_dir, f"temp_{ospath.basename(file)}_{self._gid}")
            cmd = [FFMPEG_NAME, '-i', file]
            selections = self.data.get('streams_to_remove', [])
            streams = streams_per_file[file]
            if selections:
                kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in selections and s['codec_type'] != 'video']
                cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
            else:
                cmd.extend(('-map', '0', '-map', '-0:a'))
            cmd.extend(('-c', 'copy', outfile, '-y'))
            if await self._run_cmd(cmd):
                temp_files.append(outfile)
            else:
                LOGGER.error(f"Failed to process {file} for track removal.")
                await gather(*[clean_target(f) for f in temp_files])
                await self._cleanup()
                return self._up_path
            self.data.clear()  # Reset for next file

        if not temp_files:
            LOGGER.error("No files processed successfully for merging.")
            await self._cleanup()
            return self._up_path

        self.outfile = ospath.join(base_dir, self.name)
        self._files = temp_files
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
        try:
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join([f"file '{f}'" for f in temp_files]))
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
            if not await self._run_cmd(cmd, 'direct'):
                LOGGER.error("Failed to merge processed files.")
                await self._cleanup()
                return self._up_path
            if not self.is_cancel:
                await gather(*[clean_target(f) for f in temp_files])
            result = await self._final_path()
            if self.is_cancel or not result:
                await self._cleanup()
            return result
        except Exception as e:
            LOGGER.error(f"Error merging processed files: {e}")
            await self._cleanup()
            return self._up_path
        finally:
            await clean_target(input_file)

    async def _rm_audio_single(self):
        streams, _ = await get_metavideo(self.path)
        if not streams:
            LOGGER.error(f"No streams found in {self.path}")
            await self._cleanup()
            return self._up_path
        self._start_handler(streams)
        try:
            await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
            LOGGER.info(f"Event received in _rm_audio_single, selections: {self.data.get('streams_to_remove', [])}")
            if self.is_cancel or not self.data:
                await self._cleanup()
                return self._up_path

            async with file_lock:
                base_dir = await self._name_base_dir(self.path, 'RemoveAudio')
                self.outfile = ospath.join(base_dir, self.name)
                cmd = [FFMPEG_NAME, '-i', self.path]
                selections = self.data.get('streams_to_remove', [])
                if selections:
                    kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in selections and s['codec_type'] != 'video']
                    cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
                else:
                    cmd.extend(('-map', '0', '-map', '-0:a'))
                cmd.extend(('-c', 'copy', self.outfile, '-y'))
                if not await self._run_cmd(cmd):
                    await self._cleanup()
                    return self._up_path
                result = await self._final_path()
                if self.is_cancel or not result:
                    await self._cleanup()
                return result
        except Exception as e:
            LOGGER.error(f"Error in _rm_audio_single: {e}")
            await self._cleanup()
            return self._up_path

    async def _merge_vids(self):
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        list_files = [f"file '{file}'" for file in file_list if (await get_document_type(file))[0]]

        async with file_lock:
            self.outfile = self._up_path
            if len(list_files) > 1:
                await self._name_base_dir(self.path)
                await update_status_message(self.listener.message.chat.id)
                input_file = ospath.join(self.path, f'input_{self._gid}.txt')
                try:
                    async with aiopen(input_file, 'w') as f:
                        await f.write('\n'.join(list_files))
                    self.outfile = ospath.join(self.path, self.name)
                    cmd = [FFMPEG_NAME, '-ignore_unknown', '-f', 'concat', '-safe', '0', '-i', input_file, '-map', '0', '-c', 'copy', self.outfile, '-y']
                    if not await self._run_cmd(cmd, 'direct'):
                        await self._cleanup()
                        return self._up_path
                    result = await self._final_path()
                    if self.is_cancel or not result:
                        await self._cleanup()
                    return result
                finally:
                    await clean_target(input_file)
            await self._cleanup()
            return self._up_path

    async def _merge_auds(self):
        main_video = None
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        for file in file_list:
            is_video, is_audio, _ = await get_document_type(file)
            if is_video and not main_video:
                main_video = file
            if is_audio:
                self.size += await get_path_size(file)
                self._files.append(file)

        async with file_lock:
            if main_video:
                self._files.insert(0, main_video)
                self.outfile = self._up_path
                if len(self._files) > 1:
                    _, size = await gather(self._name_base_dir(self.path), get_path_size(main_video))
                    self.size += size
                    await update_status_message(self.listener.message.chat.id)
                    cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown']
                    for i in self._files:
                        cmd.extend(('-i', i))
                    cmd.extend(('-map', '0:v:0?', '-map', '0:a:?'))
                    for j in range(1, len(self._files)):
                        cmd.extend(('-map', f'{j}:a'))
                    streams = (await get_metavideo(main_video))[0]
                    audio_track = len([i for i in range(len(streams)) if streams[i]['codec_type'] == 'audio'])
                    cmd.extend((f'-disposition:s:a:{audio_track}', 'default', '-map', '0:s:?', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'copy', self.outfile, '-y'))
                    if not await self._run_cmd(cmd, 'direct'):
                        await self._cleanup()
                        return self._up_path
                    result = await self._final_path()
                    if self.is_cancel or not result:
                        await self._cleanup()
                    return result
            await self._cleanup()
            return self._up_path

    async def _merge_subs(self, **kwargs):
        main_video = None
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        for file in file_list:
            is_video, is_sub = (await get_document_type(file))[0], file.endswith(('.ass', '.srt', '.vtt'))
            if is_video and not main_video:
                main_video = file
            if is_sub:
                self.size += await get_path_size(file)
                self._files.append(file)

        async with file_lock:
            if main_video:
                self._files.insert(0, main_video)
                self.outfile = self._up_path
                if len(self._files) > 1:
                    _, size = await gather(self._name_base_dir(self.path), get_path_size(main_video))
                    self.size += size
                    cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y']
                    self.outfile, status = ospath.join(self.path, self.name), 'direct'
                    if kwargs.get('hardsub'):
                        self.path, status = self._files[0], 'prog'
                        cmd.extend(('-i', self.path, '-vf'))
                        fontname = kwargs.get('fontname', '').replace('_', ' ') or config_dict['HARDSUB_FONT_NAME']
                        fontsize = f',FontSize={kwargs.get("fontsize") or config_dict["HARDSUB_FONT_SIZE"]}'
                        fontcolour = f',PrimaryColour=&H{kwargs["fontcolour"]}' if kwargs.get('fontcolour') else ''
                        boldstyle = ',Bold=1' if kwargs.get('boldstyle') else ''
                        quality = f',scale={self._qual[kwargs["quality"]]}:-2' if kwargs.get('quality') else ''
                        cmd.append(f"subtitles='{self._files[1]}':force_style='FontName={fontname},Shadow=1.5{fontsize}{fontcolour}{boldstyle}'{quality},unsharp,eq=contrast=1.07")
                        if config_dict['VIDTOOLS_FAST_MODE']:
                            cmd.extend(('-preset', config_dict['LIB264_PRESET'], '-c:v', 'libx264', '-crf', '24', '-map', '0:a:?', '-c:a', 'copy'))
                        else:
                            cmd.extend(('-preset', config_dict['LIB265_PRESET'], '-c:v', 'libx265', '-pix_fmt', 'yuv420p10le', '-crf', '24', '-profile:v', 'main10', '-x265-params', 'no-info=1', '-bsf:v', 'filter_units=remove_types=6', '-c:a', 'aac', '-b:a', '160k', '-map', '0:1'))
                        cmd.extend(['-map', '0:v:0?', '-map', '-0:s', self.outfile])
                    else:
                        for i in self._files:
                            cmd.extend(('-i', i))
                        cmd.extend(('-map', '0:v:0?', '-map', '0:a:?', '-map', '0:s:?'))
                        for j in range(1, len(self._files)):
                            cmd.extend(('-map', f'{j}:s'))
                        cmd.extend(('-c:v', 'copy', '-c:a', 'copy', '-c:s', 'srt', self.outfile))
                    if not await self._run_cmd(cmd, status):
                        await self._cleanup()
                        return self._up_path
                    result = await self._final_path()
                    if self.is_cancel or not result:
                        await self._cleanup()
                    return result
            await self._cleanup()
            return self._up_path

    async def _vid_trimmer(self, start_time, end_time):
        await self._queue(True)
        if self.is_cancel:
            await self._cleanup()
            return self._up_path
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path

        async with file_lock:
            for file in file_list:
                self.path = file
                if self._metadata:
                    base_dir = self.listener.dir
                    await makedirs(base_dir, exist_ok=True)
                else:
                    base_dir, self.size = await gather(self._name_base_dir(self.path, 'Trim', len(file_list) > 1), get_path_size(self.path))
                self.outfile = ospath.join(base_dir, self.name)
                self._files.append(self.path)
                cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-i', self.path, '-ss', start_time, '-to', end_time,
                       '-map', '0:v:0?', '-map', '0:a:?', '-map', '0:s:?', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'copy', self.outfile, '-y']
                if not await self._run_cmd(cmd):
                    await self._cleanup()
                    return self._up_path
            result = await self._final_path()
            if self.is_cancel or not result:
                await self._cleanup()
            return result

    async def _subsync(self, type: str='sync_manual'):
        if not self._is_dir:
            await self._cleanup()
            return self._up_path
        self.size = await get_path_size(self.path)
        list_files = natsorted(await listdir(self.path))
        if len(list_files) <= 1:
            await self._cleanup()
            return self._up_path

        async with file_lock:
            sub_files, ref_files = [], []
            if type == 'sync_manual':
                self.data = {'list': {}, 'final': {}}
                for i, file in enumerate(list_files, 1):
                    file_path = ospath.join(self.path, file)
                    if (await get_document_type(file_path))[0] or file.endswith(('.srt', '.ass')):
                        self.data['list'][i] = file
                if not self.data['list']:
                    await self._cleanup()
                    return self._up_path
                self._start_handler()
                try:
                    await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                    if self.is_cancel or not self.data.get('final'):
                        await self._cleanup()
                        return self._up_path
                    sub_files = [ospath.join(self.path, key['file']) for key in self.data['final'].values()]
                    ref_files = [ospath.join(self.path, key['ref']) for key in self.data['final'].values()]
                except Exception as e:
                    LOGGER.error(f"Error in subsync manual: {e}")
                    await self._cleanup()
                    return self._up_path
            else:
                for file in list_files:
                    file_ = ospath.join(self.path, file)
                    is_video, is_audio, _ = await get_document_type(file_)
                    if is_video or is_audio:
                        ref_files.append(file_)
                    elif file_.lower().endswith(('.srt', '.ass')):
                        sub_files.append(file_)
                if not sub_files or not ref_files:
                    await self._cleanup()
                    return self._up_path

            for sub_file, ref_file in zip(sub_files, ref_files):
                self._files.extend((sub_file, ref_file))
                self.size = await get_path_size(ref_file)
                self.name = ospath.basename(sub_file)
                name, ext = ospath.splitext(sub_file)
                if not await self._run_cmd(['alass', '--allow-negative-timestamps', ref_file, sub_file, f'{name}_SYNC.{ext}'], 'direct'):
                    await self._cleanup()
                    return self._up_path
            result = await self._final_path(self._up_path)
            if self.is_cancel or not result:
                await self._cleanup()
            return result

    async def _vid_compress(self, quality=None):
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        multi = len(file_list) > 1

        async with file_lock:
            if self._metadata:
                base_dir = self.listener.dir
                await makedirs(base_dir, exist_ok=True)
                streams = self._metadata[0]
            else:
                main_video = file_list[0]
                base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Compress', multi),
                                                                 get_metavideo(main_video), get_path_size(main_video))
            self._start_handler(streams)
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                await self._queue()
                if self.is_cancel or not isinstance(self.data, dict):
                    await self._cleanup()
                    return self._up_path

                for file in file_list:
                    self.path = file
                    if not self._metadata:
                        self.size = await get_path_size(self.path)
                    self.outfile = ospath.join(base_dir, self.name)
                    self._files.append(self.path)
                    cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-preset', config_dict['LIB265_PRESET'], '-c:v', 'libx265',
                           '-pix_fmt', 'yuv420p10le', '-crf', '24', '-profile:v', 'main10', '-map', f'0:{self.data["video"]}', '-map', '0:s:?', '-c:s', 'copy']
                    if banner := config_dict['COMPRESS_BANNER']:
                        sub_file = ospath.join(base_dir, 'subtitle.srt')
                        self._files.append(sub_file)
                        quality = f',scale={self._qual[quality]}:-2' if quality else ''
                        async with aiopen(sub_file, 'w') as f:
                            await f.write(f'1\n00:00:03,000 --> 00:00:08,00\n{banner}')
                        cmd.extend(('-vf', f"subtitles='{sub_file}'{quality},unsharp,eq=contrast=1.07", '-metadata', f'title={banner}', '-metadata:s:v',
                                    f'title={banner}', '-x265-params', 'no-info=1', '-bsf:v', 'filter_units=remove_types=6'))
                    elif quality:
                        cmd.extend(('-vf', f'scale={self._qual[quality]}:-2'))
                    cmd.extend(('-c:a', 'aac', '-b:a', '160k', '-map', f'0:{self.data["audio"]}?', self.outfile))
                    if not await self._run_cmd(cmd):
                        await self._cleanup()
                        return self._up_path
                result = await self._final_path()
                if self.is_cancel or not result:
                    await self._cleanup()
                return result
            except Exception as e:
                LOGGER.error(f"Error in _vid_compress: {e}")
                await self._cleanup()
                return self._up_path

    async def _vid_marker(self, **kwargs):
        await self._queue(True)
        if self.is_cancel:
            await self._cleanup()
            return self._up_path
        wmpath = ospath.join('watermark', f'{self.listener.mid}.png')
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path

        async with file_lock:
            for file in file_list:
                self.path = file
                self._files.append(self.path)
                if self._metadata:
                    base_dir = self.listener.dir
                    await makedirs(base_dir, exist_ok=True)
                    fsize = self.size
                else:
                    base_dir, fsize = await gather(self._name_base_dir(self.path, 'Marker', len(file_list) > 1), get_path_size(self.path))
                self.size = fsize + await get_path_size(wmpath)
                self.outfile = ospath.join(base_dir, self.name)
                wmsize, wmposition, popupwm = kwargs.get('wmsize'), kwargs.get('wmposition'), kwargs.get('popupwm') or ''
                if popupwm:
                    duration = (await get_media_info(self.path))[0]
                    popupwm = f':enable=lt(mod(t\,{duration}/{popupwm})\,20)'
                hardusb, subfile = kwargs.get('hardsub') or '', kwargs.get('subfile', '')
                if hardusb and await aiopath.exists(subfile):
                    fontname = kwargs.get('fontname', '').replace('_', ' ') or config_dict['HARDSUB_FONT_NAME']
                    fontsize = f',FontSize={kwargs.get("fontsize") or config_dict["HARDSUB_FONT_SIZE"]}'
                    fontcolour = f',PrimaryColour=&H{kwargs["fontcolour"]}' if kwargs.get('fontcolour') else ''
                    boldstyle = ',Bold=1' if kwargs.get('boldstyle') else ''
                    hardusb = f",subtitles='{subfile}':force_style='FontName={fontname},Shadow=1.5{fontsize}{fontcolour}{boldstyle}',unsharp,eq=contrast=1.07"
                quality = f',scale={self._qual[kwargs["quality"]]}:-2' if kwargs.get('quality') else ''
                cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-i', wmpath, '-filter_complex',
                       f"[1][0]scale2ref=w='iw*{wmsize}/100':h='ow/mdar'[wm][vid];[vid][wm]overlay={wmposition}{popupwm}{quality}{hardusb}"]
                if config_dict['VIDTOOLS_FAST_MODE']:
                    cmd.extend(('-c:v', 'libx264', '-preset', config_dict['LIB264_PRESET'], '-crf', '25'))
                cmd.extend(('-map', '0:a:?', '-map', '0:s:?', '-c:a', 'copy', '-c:s', 'copy', self.outfile))
                if not await self._run_cmd(cmd):
                    await self._cleanup()
                    return self._up_path
            await gather(clean_target(wmpath), clean_target(subfile))
            result = await self._final_path()
            if self.is_cancel or not result:
                await self._cleanup()
            return result

    async def _vid_extract(self):
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        if self._metadata:
            base_dir = ospath.join(self.listener.dir, self.name.split('.', 1)[0])
            await makedirs(base_dir, exist_ok=True)
            streams = self._metadata[0]
        else:
            main_video = file_list[0]
            base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Extract', len(file_list) > 1),
                                                             get_metavideo(main_video), get_path_size(main_video))

        self._start_handler(streams)
        try:
            await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
            await self._queue()
            if self.is_cancel or not self.data:
                await self._cleanup()
                return self._up_path

            async with file_lock:
                if await aiopath.isfile(self._up_path) or self._metadata:
                    base_name = self.name if self._metadata else ospath.basename(self.path)
                    self._up_path = ospath.join(base_dir, f'{base_name.rsplit(".", 1)[0]} (EXTRACT)')
                    await makedirs(self._up_path, exist_ok=True)
                    base_dir = self._up_path

                task_files = []
                for file in file_list:
                    self.path = file
                    if not self._metadata:
                        self.size = await get_path_size(self.path)
                    base_name = self.name if self._metadata else ospath.basename(self.path).rsplit('.', 1)[0]
                    extension = dict(zip(['audio', 'subtitle', 'video'], self.data['extension']))

                    def _build_command(stream_data):
                        cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-i', self.path, '-map', f'0:{stream_data["index"]}']
                        if self.data.get('alt_mode'):
                            if stream_data['codec_type'] == 'audio':
                                cmd.extend(('-b:a', '156k'))
                            elif stream_data['codec_type'] == 'video':
                                cmd.extend(('-c', 'copy'))
                        else:
                            cmd.extend(('-c', 'copy'))
                        cmd.extend((self.outfile, '-y'))
                        return cmd

                    keys = self.data['key']
                    if isinstance(keys, int):
                        stream_data = self.data['streams'][keys]
                        self.name = f'{base_name}_{self._format_stream_name(stream_data).replace(" ~ ", "_").replace("(", "").replace(")", "").replace(" ", "_")}.{extension[stream_data["codec_type"]]}'
                        self.outfile = ospath.join(base_dir, self.name)
                        if not await self._run_cmd(_build_command(stream_data)):
                            await self._cleanup()
                            return self._up_path
                        task_files.append(file)
                    else:
                        for stream_data in self.data['streams'].values():
                            for key in keys:
                                if key == stream_data['codec_type']:
                                    self.name = f'{base_name}_{self._format_stream_name(stream_data).replace(" ~ ", "_").replace("(", "").replace(")", "").replace(" ", "_")}.{extension[key]}'
                                    self.outfile = ospath.join(base_dir, self.name)
                                    if not await self._run_cmd(_build_command(stream_data)):
                                        await self._cleanup()
                                        return self._up_path
                                    task_files.append(file)
                await gather(*[clean_target(file) for file in task_files])
                result = await self._final_path(self._up_path)
                if self.is_cancel or not result:
                    await self._cleanup()
                return result
        except Exception as e:
            LOGGER.error(f"Error in _vid_extract: {e}")
            await self._cleanup()
            return self._up_path

    def _format_stream_name(self, stream):
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height', '') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    async def _vid_convert(self):
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        multi = len(file_list) > 1

        async with file_lock:
            if self._metadata:
                base_dir = self.listener.dir
                await makedirs(base_dir, exist_ok=True)
                streams = self._metadata[0]
            else:
                main_video = file_list[0]
                base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Convert', multi),
                                                                 get_metavideo(main_video), get_path_size(main_video))
            self._start_handler(streams)
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                await self._queue()
                if self.is_cancel or not self.data:
                    await self._cleanup()
                    return self._up_path

                for file in file_list:
                    self.path = file
                    if not self._metadata:
                        self.size = await get_path_size(self.path)
                    self.outfile = ospath.join(base_dir, self.name)
                    self._files.append(self.path)
                    cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-map', '0:v:0',
                           '-vf', f'scale={self._qual[self.data]}:-2', '-map', '0:a:?', '-map', '0:s:?', '-c:a', 'copy', '-c:s', 'copy', self.outfile]
                    if not await self._run_cmd(cmd):
                        await self._cleanup()
                        return self._up_path
                result = await self._final_path()
                if self.is_cancel or not result:
                    await self._cleanup()
                return result
            except Exception as e:
                LOGGER.error(f"Error in _vid_convert: {e}")
                await self._cleanup()
                return self._up_path

    async def _rm_stream(self):
        file_list = await self._get_files()
        if not file_list:
            await self._cleanup()
            return self._up_path
        multi = len(file_list) > 1

        async with file_lock:
            if self._metadata:
                base_dir = self.listener.dir
                await makedirs(base_dir, exist_ok=True)
                streams = self._metadata[0]
            else:
                main_video = file_list[0]
                base_dir, (streams, _), self.size = await gather(self._name_base_dir(main_video, 'Remove', multi),
                                                                 get_metavideo(main_video), get_path_size(main_video))
            self._start_handler(streams)
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                LOGGER.info(f"Event received in _rm_stream, selections: {self.data.get('sdata', [])}")
                await self._queue()
                if self.is_cancel or not self.data:
                    await self._cleanup()
                    return self._up_path

                for file in file_list:
                    self.path = file
                    if not self._metadata:
                        self.size = await get_path_size(self.path)
                    self.outfile = ospath.join(base_dir, self.name)
                    self._files.append(self.path)
                    cmd = [FFMPEG_NAME, '-hide_banner', '-y', '-ignore_unknown', '-i', self.path]
                    if self.data.get('sdata'):
                        kept_streams = [f'0:{s["index"]}' for s in self.data['streams'].values() if s['index'] not in self.data['sdata']]
                        cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
                    else:
                        cmd.extend(['-map', '0'])  # Keep all if no selection
                    cmd.extend(('-c', 'copy', self.outfile))
                    if not await self._run_cmd(cmd):
                        await self._cleanup()
                        return self._up_path
                result = await self._final_path()
                if self.is_cancel or not result:
                    await self._cleanup()
                return result
            except Exception as e:
                LOGGER.error(f"Error in _rm_stream: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path
