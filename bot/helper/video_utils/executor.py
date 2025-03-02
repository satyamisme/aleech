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
    """Fetches metadata from a video file using ffprobe."""
    async with io_lock:
        try:
            stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_format', '-show_streams', video_file])
            if rcode != 0:
                LOGGER.error(f"ffprobe error for {video_file}: {stderr}")
                return [], {}
            metadata = literal_eval(stdout)
            LOGGER.info(f"Retrieved metadata for {video_file}")
            return metadata.get('streams', []), metadata.get('format', {})
        except Exception as e:
            LOGGER.error(f"Error in get_metavideo for {video_file}: {e}", exc_info=True)
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
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {path}")

    async def _cleanup(self):
        """Cleans up all temporary files and directories."""
        async with file_lock:
            try:
                await gather(*[clean_target(file) for file in self._files if await aiopath.exists(file)])
                self._files.clear()
                input_file = ospath.join(self.path, f'input_{self._gid}.txt')
                if await aiopath.exists(input_file):
                    await clean_target(input_file)
                    LOGGER.info(f"Cleaned up input file: {input_file}")
                for extracted_dir in [d for d in self._files if await aiopath.isdir(d)]:
                    if await aiopath.exists(extracted_dir):
                        await rmtree(extracted_dir, ignore_errors=True)
                        LOGGER.info(f"Removed extracted directory: {extracted_dir}")
                if self.is_cancel:  # Only clear data if explicitly cancelled
                    self.data.clear()
                self.event.clear()
                self.is_cancel = True
                LOGGER.info(f"Cleanup completed for {self.mode}")
            except Exception as e:
                LOGGER.error(f"Cleanup error for {self.mode}: {e}", exc_info=True)

    async def _queue(self, update=False):
        """Queues the task if necessary, with status updates."""
        if self._metadata:
            add_to_queue, event = await check_running_tasks(self.listener.mid)
            if add_to_queue:
                LOGGER.info(f'Added to Queue/Download: {self.name} (MID: {self.listener.mid})')
                async with task_dict_lock:
                    task_dict[self.listener.mid] = QueueStatus(self.listener, self.size, self._gid, 'dl')
                await self.listener.onDownloadStart()
                if update:
                    await sendStatusMessage(self.listener.message)
                try:
                    await wait_for(event.wait(), timeout=300)
                    LOGGER.info(f"Queue completed for {self.name}")
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
                LOGGER.info(f"Added to non_queued_dl: {self.listener.mid}")

    async def _extract_zip(self, zip_path):
        """Extracts ZIP or RAR files and returns the directory if media files are found."""
        async with zip_lock:
            extract_dir = ospath.join(ospath.dirname(zip_path), f"{ospath.splitext(ospath.basename(zip_path))[0]}_{int(time())}")
            try:
                if not await aiopath.exists(extract_dir):
                    await makedirs(extract_dir, exist_ok=True)
                    LOGGER.info(f"Created extraction directory: {extract_dir}")
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
                            LOGGER.info(f"Extracted valid media file: {file_path}")
                            return extract_dir
                LOGGER.warning(f"No valid media files found in {extract_dir}")
                await rmtree(extract_dir, ignore_errors=True)
                await clean_target(zip_path)
                return None
            except Exception as e:
                LOGGER.error(f"ZIP extraction error for {zip_path}: {e}", exc_info=True)
                if await aiopath.exists(extract_dir):
                    await rmtree(extract_dir, ignore_errors=True)
                await clean_target(zip_path)
                return None

    async def _get_files(self):
        """Collects all valid media files from the provided path."""
        file_list = []
        async with io_lock:
            if self._metadata:
                file_list.append(self.path)
                LOGGER.info(f"Using metadata path: {self.path}")
            elif await aiopath.isfile(self.path):
                if (await get_document_type(self.path))[0]:
                    file_list.append(self.path)
                    LOGGER.info(f"Single media file: {self.path}")
                elif self.path.lower().endswith(('.zip', '.rar')):
                    extract_dir = await self._extract_zip(self.path)
                    if extract_dir:
                        self.path = extract_dir
                        for dirpath, _, files in await sync_to_async(walk, extract_dir):
                            for file in natsorted(files):
                                file_path = ospath.join(dirpath, file)
                                if (await get_document_type(file_path))[0]:
                                    file_list.append(file_path)
                        self._files.append(extract_dir)
                        LOGGER.info(f"Extracted ZIP files: {file_list}")
            else:
                for dirpath, _, files in await sync_to_async(walk, self.path):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            file_list.append(file_path)
                            LOGGER.info(f"Found media file in directory: {file_path}")
                        elif file_path.lower().endswith(('.zip', '.rar')):
                            extract_dir = await self._extract_zip(file_path)
                            if extract_dir:
                                for sub_dirpath, _, sub_files in await sync_to_async(walk, extract_dir):
                                    for sub_file in natsorted(sub_files):
                                        if (await get_document_type(ospath.join(sub_dirpath, sub_file)))[0]:
                                            file_list.append(ospath.join(sub_dirpath, sub_file))
                                self._files.append(extract_dir)
                                LOGGER.info(f"Extracted nested ZIP files: {file_list}")
            if not file_list:
                LOGGER.error(f"No valid files found for {self.mode}")
            return file_list

    async def execute(self):
        """Executes the specified video processing mode."""
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, kwargs = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}", exc_info=True)
            await self._cleanup()
            return self._up_path
        LOGGER.info(f"Executing {self.mode} with name: {self.name}, kwargs: {kwargs}")
        if not self._metadata and self.mode in config_dict.get('DISABLE_MULTI_VIDTOOLS', []):
            path = await self._get_files()
            self.path = path[0] if path else self._up_path
            LOGGER.info(f"Single file mode adjusted path: {self.path}")
        if self._metadata:
            if not self.name:
                self.name = get_url_name(self.path)
                LOGGER.info(f"Generated name from URL: {self.name}")
            if not self.name.upper().endswith(('MP4', 'MKV')):
                self.name += '.mkv'
            try:
                self.size = int(self._metadata[1]['size'])
            except Exception as e:
                LOGGER.error(f"Invalid metadata: {e}", exc_info=True)
                await self._cleanup()
                await self.listener.onDownloadError('Invalid data, check the link!')
                return self._up_path

        try:
            result = {
                'vid_vid': self._merge_vids,
                'vid_aud': self._merge_auds,
                'vid_sub': lambda: self._merge_subs(**kwargs),
                'trim': lambda: self._vid_trimmer(**kwargs),
                'watermark': lambda: self._vid_marker(**kwargs),
                'compress': lambda: self._vid_compress(**kwargs),
                'subsync': lambda: self._subsync(**kwargs),
                'rmstream': self._rm_stream,
                'extract': self._vid_extract,
                'merge_rmaudio': self._merge_and_rmaudio,
                'merge_preremove_audio': self._merge_preremove_audio,
            }.get(self.mode, self._vid_convert)()
            LOGGER.info(f"{self.mode} completed with result: {result}")
            return await result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}", exc_info=True)
            await self._cleanup()
            return self._up_path
        finally:
            if self.is_cancel:
                await self._cleanup()

    @new_task
    async def _start_handler(self, *args):
        """Triggers the ExtraSelect UI for stream selection."""
        LOGGER.info(f"Starting handler for {self.mode} with args: {args}")
        await sleep(0.5)
        try:
            await ExtraSelect(self).get_buttons(*args)
        except Exception as e:
            LOGGER.error(f"Error in _start_handler for {self.mode}: {e}", exc_info=True)
            self.is_cancel = True
            await self._cleanup()

    async def _send_status(self, status='wait'):
        """Sends status updates to the Telegram UI."""
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update for {self.mode}: {status}")
        except Exception as e:
            LOGGER.error(f"Failed to send status for {self.mode}: {e}", exc_info=True)

    async def _final_path(self, outfile=''):
        """Sets the final output path and ensures cleanup of all but the final file."""
        async with file_lock:
            try:
                if self._metadata:
                    self._up_path = outfile or self.outfile
                    LOGGER.info(f"Metadata final path: {self._up_path}")
                else:
                    scan_dir = self._up_path if self._is_dir else ospath.split(self._up_path)[0]
                    for dirpath, _, files in await sync_to_async(walk, scan_dir):
                        for file in files:
                            if file != ospath.basename(outfile or self.outfile):
                                await clean_target(ospath.join(dirpath, file))
                                LOGGER.info(f"Cleaned non-final file: {file}")
                    all_files = [(dirpath, file) for dirpath, _, files in await sync_to_async(walk, scan_dir) for file in files]
                    if len(all_files) == 1:
                        self._up_path = ospath.join(*all_files[0])
                        LOGGER.info(f"Single file final path: {self._up_path}")
                    elif len(all_files) > 1:
                        LOGGER.info(f"Multiple files remain after processing, keeping only final output: {all_files}")
                for extracted_dir in self._files[:]:
                    if await aiopath.isdir(extracted_dir) and await aiopath.exists(extracted_dir):
                        await rmtree(extracted_dir, ignore_errors=True)
                        LOGGER.info(f"Removed extracted dir in final path: {extracted_dir}")
                self._files.clear()
                LOGGER.info(f"Final path set to: {self._up_path}")
                return self._up_path
            except Exception as e:
                LOGGER.error(f"Final path cleanup error for {self.mode}: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path

    async def _name_base_dir(self, path, info: str=None, multi: bool=False):
        """Generates the output filename and base directory."""
        async with file_lock:
            base_dir, file_name = ospath.split(path)
            if not self.name or multi:
                if info:
                    if await aiopath.isfile(path):
                        file_name = file_name.rsplit('.', 1)[0]
                    file_name += f'_{info}.mkv'
                    LOGGER.info(f"Generated name with info: {file_name}")
                self.name = file_name
            if not self.name.upper().endswith(('MP4', 'MKV')):
                self.name += '.mkv'
            LOGGER.info(f"Set name: {self.name} with base_dir: {base_dir}")
            return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        """Runs FFmpeg commands with progress monitoring."""
        try:
            await self._send_status(status)
            LOGGER.info(f"Running FFmpeg command for {self.mode}: {' '.join(cmd)}")
            self.listener.suproc = await create_subprocess_exec(*cmd, stderr=PIPE)
            _, code = await gather(self.progress(status), self.listener.suproc.wait())
            if code == 0:
                if not self.listener.seed:
                    await gather(*[clean_target(file) for file in self._files if await aiopath.exists(file)])
                    LOGGER.info(f"Cleaned up temporary files after successful FFmpeg run")
                self._files.clear()
                LOGGER.info(f"FFmpeg command succeeded for {self.mode}")
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancel = True
                await self._cleanup()
                LOGGER.info(f"FFmpeg cancelled for {self.mode}")
            else:
                error_msg = (await self.listener.suproc.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error for {self.mode}: {error_msg}")
                self.is_cancel = True
                await self._cleanup()
            return False
        except Exception as e:
            LOGGER.error(f"Run command error for {self.mode}: {e}", exc_info=True)
            self.is_cancel = True
            await self._cleanup()
            return False

    async def _merge_and_rmaudio(self):
        """Merges multiple files and removes selected streams post-merge."""
        file_list = await self._get_files()
        if not file_list:
            LOGGER.error("No valid video files found for merging.")
            await self._cleanup()
            return self._up_path
        if len(file_list) == 1:
            self.path = file_list[0]
            return await self._rm_audio_single()

        self.size = sum(await gather(*[get_path_size(f) for f in file_list]))
        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', True)

        async with file_lock:
            self.outfile = ospath.join(base_dir, f"temp_merge_{self._gid}.mkv")
            self._files = file_list
            input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
            try:
                async with aiopen(input_file, 'w') as f:
                    await f.write('\n'.join([f"file '{f}'" for f in file_list]))
                LOGGER.info(f"Created input file: {input_file}")
                cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
                if not await self._run_cmd(cmd, 'direct'):
                    LOGGER.error("Failed to merge files due to FFmpeg error.")
                    await self._cleanup()
                    return self._up_path
            except Exception as e:
                LOGGER.error(f"Error creating/running merge for {input_file}: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path
            finally:
                if await aiopath.exists(input_file):
                    await clean_target(input_file)
                    LOGGER.info(f"Cleaned up input file: {input_file}")

        streams, _ = await get_metavideo(self.outfile)
        if not streams:
            LOGGER.error(f"No streams detected in merged file: {self.outfile}")
            await self._cleanup()
            return self._up_path

        await self._start_handler(streams)
        try:
            await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
            LOGGER.info(f"Event received in _merge_and_rmaudio, selections: {self.data.get('streams_to_remove', [])}")
        except TimeoutError:
            LOGGER.error(f"Timeout waiting for event in _merge_and_rmaudio")
            self.is_cancel = True
            await self._cleanup()
            return self._up_path

        if self.is_cancel or not self.data:
            LOGGER.warning(f"Merge and remove audio cancelled or no selections made: is_cancel={self.is_cancel}, data={self.data}")
            await self._cleanup()
            return self.outfile

        streams_to_remove = self.data.get('streams_to_remove', [])
        if not streams_to_remove:
            LOGGER.info("No streams selected to remove, keeping all.")
            return await self._final_path(self.outfile)

        final_outfile = ospath.join(base_dir, self.name)
        cmd = [FFMPEG_NAME, '-i', self.outfile]
        kept_streams = [f'0:{s["index"]}' for s in self.data['streams'].values() 
                        if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
        cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
        cmd.extend(('-c', 'copy', final_outfile, '-y'))
        LOGGER.info(f"Executing FFmpeg remove command: {' '.join(cmd)}")
        if not await self._run_cmd(cmd, 'direct'):
            LOGGER.error("Failed to remove selected tracks from merged file.")
            await self._cleanup()
            return self.outfile

        if await aiopath.exists(self.outfile):
            await clean_target(self.outfile)
            LOGGER.info(f"Cleaned up temporary merged file: {self.outfile}")
        return await self._final_path(final_outfile)

    async def _merge_preremove_audio(self):
        """Removes selected streams from each file before merging."""
        file_list = await self._get_files()
        if not file_list:
            LOGGER.error("No valid video files found for processing.")
            await self._cleanup()
            return self._up_path
        if len(file_list) == 1:
            self.path = file_list[0]
            return await self._rm_audio_single()

        stream_data = await gather(*[get_metavideo(f) for f in file_list])
        streams_per_file = {f: streams for f, (streams, _) in zip(file_list, stream_data) if streams}
        self.size = sum(await gather(*[get_path_size(f) for f in file_list]))
        base_dir = await self._name_base_dir(file_list[0], 'Merge-PreRemoveAudio', True)

        temp_files = []
        for file in file_list:
            if file not in streams_per_file:
                LOGGER.warning(f"No streams found in {file}, skipping.")
                continue
            await self._start_handler(streams_per_file[file])
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                LOGGER.info(f"Event received for {file} in _merge_preremove_audio, selections: {self.data.get('streams_to_remove', [])}")
            except TimeoutError:
                LOGGER.error(f"Timeout waiting for event for {file}")
                await self._cleanup()
                return self._up_path
            if self.is_cancel or not self.data:
                LOGGER.warning(f"Cancelled or no data for {file}: is_cancel={self.is_cancel}, data={self.data}")
                await self._cleanup()
                return self._up_path

            outfile = ospath.join(base_dir, f"temp_{ospath.basename(file)}_{self._gid}")
            cmd = [FFMPEG_NAME, '-i', file]
            selections = self.data.get('streams_to_remove', [])
            streams = streams_per_file[file]
            if selections:
                kept_streams = [f'0:{s["index"]}' for s in streams if f"{file}_{s['index']}" not in selections and s['codec_type'] != 'video']
                cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
            else:
                cmd.extend(('-map', '0', '-map', '-0:a'))
            cmd.extend(('-c', 'copy', outfile, '-y'))
            if await self._run_cmd(cmd):
                temp_files.append(outfile)
                LOGGER.info(f"Processed file {file} to {outfile}")
            else:
                LOGGER.error(f"Failed to process {file} for track removal.")
                await gather(*[clean_target(f) for f in temp_files if await aiopath.exists(f)])
                await self._cleanup()
                return self._up_path
            self.data.clear()
            self.event.clear()

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
            LOGGER.info(f"Created merge input file: {input_file}")
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
            if not await self._run_cmd(cmd, 'direct'):
                LOGGER.error("Failed to merge processed files.")
                await self._cleanup()
                return self._up_path
            if not self.is_cancel:
                await gather(*[clean_target(f) for f in temp_files if await aiopath.exists(f)])
                LOGGER.info(f"Cleaned up temp files: {temp_files}")
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error merging processed files: {e}", exc_info=True)
            await self._cleanup()
            return self._up_path
        finally:
            if await aiopath.exists(input_file):
                await clean_target(input_file)
                LOGGER.info(f"Cleaned up merge input file: {input_file}")

    async def _rm_audio_single(self):
        """Removes selected streams from a single video file."""
        streams, _ = await get_metavideo(self.path)
        if not streams:
            LOGGER.error(f"No streams found in {self.path}")
            await self._cleanup()
            return self._up_path
        await self._start_handler(streams)
        try:
            await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
            LOGGER.info(f"Event received in _rm_audio_single, selections: {self.data.get('streams_to_remove', [])}")
        except TimeoutError:
            LOGGER.error(f"Timeout waiting for event in _rm_audio_single")
            self.is_cancel = True
            await self._cleanup()
            return self._up_path

        if self.is_cancel or not self.data:
            LOGGER.warning(f"Cancelled or no data: is_cancel={self.is_cancel}, data={self.data}")
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
            return await self._final_path()

    async def _merge_vids(self):
        """Merges multiple video files into one."""
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
                    return await self._final_path()
                finally:
                    if await aiopath.exists(input_file):
                        await clean_target(input_file)
                        LOGGER.info(f"Cleaned up input file: {input_file}")
            await self._cleanup()
            return self._up_path

    async def _merge_auds(self):
        """Merges audio tracks into a video file."""
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
                    cmd.extend((f'-disposition:a:{audio_track}', 'default', '-map', '0:s:?', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'copy', self.outfile, '-y'))
                    if not await self._run_cmd(cmd, 'direct'):
                        await self._cleanup()
                        return self._up_path
                    return await self._final_path()
            await self._cleanup()
            return self._up_path

    async def _merge_subs(self, **kwargs):
        """Merges subtitles into a video file."""
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
                        if config_dict.get('VIDTOOLS_FAST_MODE', False):
                            cmd.extend(('-preset', config_dict['LIB264_PRESET'], '-c:v', 'libx264', '-crf', '24', '-map', '0:a:?', '-c:a', 'copy'))
                        else:
                            cmd.extend(('-preset', config_dict['LIB265_PRESET'], '-c:v', 'libx265', '-pix_fmt', 'yuv420p10le', '-crf', '24', '-profile:v', 'main10', '-x265-params', 'no-info=1', '-bsf:v', 'filter_units=remove_types=6', '-c:a', 'aac', '-b:a', '160k', '-map', '0:a?'))
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
                    return await self._final_path()
            await self._cleanup()
            return self._up_path

    async def _vid_trimmer(self, start_time, end_time):
        """Trims a video based on specified start and end times."""
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
                    LOGGER.info(f"Created metadata output dir: {base_dir}")
                else:
                    base_dir, self.size = await gather(self._name_base_dir(self.path, 'Trim', len(file_list) > 1), get_path_size(self.path))
                self.outfile = ospath.join(base_dir, self.name)
                self._files.append(self.path)
                cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-i', self.path, '-ss', start_time, '-to', end_time,
                       '-map', '0:v:0?', '-map', '0:a:?', '-map', '0:s:?', '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'copy', self.outfile, '-y']
                if not await self._run_cmd(cmd):
                    await self._cleanup()
                    return self._up_path
            return await self._final_path()

    async def _subsync(self, type: str='sync_manual'):
        """Synchronizes subtitles with video files."""
        if not self._is_dir:
            LOGGER.warning(f"{self.path} is not a directory for subsync")
            await self._cleanup()
            return self._up_path
        self.size = await get_path_size(self.path)
        list_files = natsorted(await listdir(self.path))
        if len(list_files) <= 1:
            LOGGER.warning(f"Not enough files for subsync: {list_files}")
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
                    LOGGER.warning("No valid files for subsync manual mode")
                    await self._cleanup()
                    return self._up_path
                await self._start_handler()
                try:
                    await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                    if self.is_cancel or not self.data.get('final'):
                        await self._cleanup()
                        return self._up_path
                    sub_files = [ospath.join(self.path, key['file']) for key in self.data['final'].values()]
                    ref_files = [ospath.join(self.path, key['ref']) for key in self.data['final'].values()]
                    LOGGER.info(f"Subsync files selected: {sub_files}, refs: {ref_files}")
                except Exception as e:
                    LOGGER.error(f"Error in subsync manual: {e}", exc_info=True)
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
                    LOGGER.warning(f"No valid sub/ref files for auto subsync: subs={sub_files}, refs={ref_files}")
                    await self._cleanup()
                    return self._up_path

            for sub_file, ref_file in zip(sub_files, ref_files):
                self._files.extend((sub_file, ref_file))
                self.size = await get_path_size(ref_file)
                self.name = ospath.basename(sub_file)
                name, ext = ospath.splitext(sub_file)
                output_file = f'{name}_SYNC.{ext}'
                cmd = ['alass', '--allow-negative-timestamps', ref_file, sub_file, output_file]
                if not await self._run_cmd(cmd, 'direct'):
                    await self._cleanup()
                    return self._up_path
                if await aiopath.exists(output_file):
                    LOGGER.info(f"Subsync output created: {output_file}")
            return await self._final_path(self._up_path)

    async def _vid_compress(self, quality=None):
        """Compresses a video file with selected audio."""
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
            await self._start_handler(streams)
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                await self._queue()
                if self.is_cancel or not isinstance(self.data, dict):
                    LOGGER.warning(f"Cancelled or invalid data: is_cancel={self.is_cancel}, data={self.data}")
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
                    if banner := config_dict.get('COMPRESS_BANNER', ''):
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
                return await self._final_path()
            except Exception as e:
                LOGGER.error(f"Error in _vid_compress: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path

    async def _vid_marker(self, **kwargs):
        """Adds a watermark to a video file."""
        await self._queue(True)
        if self.is_cancel:
            await self._cleanup()
            return self._up_path
        wmpath = kwargs.get('watermark_path', ospath.join('watermark', f'{self.listener.mid}.png'))
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
                if not await aiopath.exists(wmpath):
                    LOGGER.error(f"Watermark file {wmpath} does not exist")
                    await self._cleanup()
                    return self._up_path
                self.size = fsize + await get_path_size(wmpath)
                self.outfile = ospath.join(base_dir, self.name)
                wmsize, wmposition, popupwm = kwargs.get('wmsize', '10'), kwargs.get('wmposition', '5:5'), kwargs.get('popupwm', '')
                if popupwm:
                    duration = (await get_media_info(self.path))[0]
                    popupwm = f':enable=lt(mod(t\,{duration}/{popupwm})\,20)'
                hardusb, subfile = kwargs.get('hardsub', ''), kwargs.get('subfile', '')
                if hardusb and await aiopath.exists(subfile):
                    fontname = kwargs.get('fontname', '').replace('_', ' ') or config_dict['HARDSUB_FONT_NAME']
                    fontsize = f',FontSize={kwargs.get("fontsize") or config_dict["HARDSUB_FONT_SIZE"]}'
                    fontcolour = f',PrimaryColour=&H{kwargs["fontcolour"]}' if kwargs.get('fontcolour') else ''
                    boldstyle = ',Bold=1' if kwargs.get('boldstyle') else ''
                    hardusb = f",subtitles='{subfile}':force_style='FontName={fontname},Shadow=1.5{fontsize}{fontcolour}{boldstyle}',unsharp,eq=contrast=1.07"
                quality = f',scale={self._qual[kwargs["quality"]]}:-2' if kwargs.get('quality') else ''
                cmd = [FFMPEG_NAME, '-hide_banner', '-ignore_unknown', '-y', '-i', self.path, '-i', wmpath, '-filter_complex',
                       f"[1][0]scale2ref=w='iw*{wmsize}/100':h='ow/mdar'[wm][vid];[vid][wm]overlay={wmposition}{popupwm}{quality}{hardusb}"]
                if config_dict.get('VIDTOOLS_FAST_MODE', False):
                    cmd.extend(('-c:v', 'libx264', '-preset', config_dict['LIB264_PRESET'], '-crf', '25'))
                cmd.extend(('-map', '0:a:?', '-map', '0:s:?', '-c:a', 'copy', '-c:s', 'copy', self.outfile))
                if not await self._run_cmd(cmd):
                    await self._cleanup()
                    return self._up_path
            if await aiopath.exists(wmpath):
                await clean_target(wmpath)
                LOGGER.info(f"Cleaned up watermark file: {wmpath}")
            if 'subfile' in kwargs and await aiopath.exists(kwargs['subfile']):
                await clean_target(kwargs['subfile'])
                LOGGER.info(f"Cleaned up subtitle file: {kwargs['subfile']}")
            return await self._final_path()

    async def _vid_extract(self):
        """Extracts selected streams from a video file."""
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

        await self._start_handler(streams)
        try:
            await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
            await self._queue()
            if self.is_cancel or not self.data:
                LOGGER.warning(f"Cancelled or no data: is_cancel={self.is_cancel}, data={self.data}")
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
                await gather(*[clean_target(file) for file in task_files if await aiopath.exists(file)])
                return await self._final_path(self._up_path)
        except Exception as e:
            LOGGER.error(f"Error in _vid_extract: {e}", exc_info=True)
            await self._cleanup()
            return self._up_path

    def _format_stream_name(self, stream):
        """Formats stream information for file naming."""
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    async def _vid_convert(self):
        """Converts a video to a different resolution."""
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
            await self._start_handler(streams)
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                await self._queue()
                if self.is_cancel or not self.data:
                    LOGGER.warning(f"Cancelled or no data: is_cancel={self.is_cancel}, data={self.data}")
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
                return await self._final_path()
            except Exception as e:
                LOGGER.error(f"Error in _vid_convert: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path

    async def _rm_stream(self):
        """Removes selected streams from a video file."""
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
            await self._start_handler(streams)
            try:
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                LOGGER.info(f"Event received in _rm_stream, selections: {self.data.get('sdata', [])}")
                await self._queue()
                if self.is_cancel or not self.data:
                    LOGGER.warning(f"Cancelled or no data: is_cancel={self.is_cancel}, data={self.data}")
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
                        cmd.extend(['-map', '0'])
                    cmd.extend(('-c', 'copy', self.outfile))
                    if not await self._run_cmd(cmd):
                        await self._cleanup()
                        return self._up_path
                return await self._final_path()
            except Exception as e:
                LOGGER.error(f"Error in _rm_stream: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path