from __future__ import annotations
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, makedirs
from aioshutil import rmtree
from asyncio import create_subprocess_exec, sleep, gather, Lock
from asyncio.subprocess import PIPE
from natsort import natsorted
from os import path as ospath, walk
from time import time

from bot import task_dict, task_dict_lock, LOGGER, VID_MODE, FFMPEG_NAME
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec, new_task
from bot.helper.ext_utils.files_utils import get_path_size, clean_target
from bot.helper.ext_utils.links_utils import get_url_name
from bot.helper.ext_utils.media_utils import get_document_type, FFProgress
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage
from bot.helper.video_utils.extra_selector import ExtraSelect

file_lock = Lock()
io_lock = Lock()

async def get_metavideo(video_file):
    async with io_lock:
        try:
            stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_format', '-show_streams', video_file])
            if rcode != 0:
                LOGGER.error(f"ffprobe error for {video_file}: {stderr}")
                return [], {}
            from ast import literal_eval
            metadata = literal_eval(stdout)
            LOGGER.info(f"Retrieved metadata for {video_file}")
            return metadata.get('streams', []), metadata.get('format', {})
        except Exception as e:
            LOGGER.error(f"Error in get_metavideo for {video_file}: {e}", exc_info=True)
            return [], {}

class VidEcxecutor(FFProgress):
    def __init__(self, listener: task.TaskListener, path: str, gid: str, metadata=False):
        self.data = {}
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
        super().__init__()
        self.is_cancel = False
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {path}")

    async def _cleanup(self):
        async with file_lock:
            try:
                await gather(*[clean_target(f) for f in self._files if await aiopath.exists(f)])
                self._files.clear()
                input_file = ospath.join(self.path, f'input_{self._gid}.txt')
                if await aiopath.exists(input_file):
                    await clean_target(input_file)
                    LOGGER.info(f"Cleaned up input file: {input_file}")
                if self.is_cancel:
                    self.data.clear()
                self.is_cancel = True
                LOGGER.info(f"Cleanup completed for {self.mode}")
            except Exception as e:
                LOGGER.error(f"Cleanup error for {self.mode}: {e}", exc_info=True)

    async def _get_files(self):
        file_list = []
        async with io_lock:
            if self._metadata:
                file_list.append(self.path)
                LOGGER.info(f"Using metadata path: {self.path}")
            elif await aiopath.isfile(self.path):
                if (await get_document_type(self.path))[0]:
                    file_list.append(self.path)
                    LOGGER.info(f"Single media file: {self.path}")
            else:
                for dirpath, _, files in await sync_to_async(walk, self.path):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            file_list.append(file_path)
                            LOGGER.info(f"Found media file: {file_path}")
            if not file_list:
                LOGGER.error(f"No valid files found for {self.mode}")
            return file_list

    async def execute(self):
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, _ = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}", exc_info=True)
            await self._cleanup()
            return self._up_path
        LOGGER.info(f"Executing {self.mode} with name: {self.name}")
        if not self._metadata:
            path = await self._get_files()
            self.path = path[0] if path else self._up_path
            LOGGER.info(f"Adjusted path: {self.path}")
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
                await self.listener.onDownloadError('Invalid data!')
                return self._up_path

        try:
            result = {
                'merge_rmaudio': self._merge_and_rmaudio,
                'merge_preremove_audio': self._merge_preremove_audio,
            }[self.mode]()
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
        LOGGER.info(f"Starting handler for {self.mode} with args: {args}")
        await sleep(0.5)
        try:
            await ExtraSelect(self).get_buttons(*args)
            LOGGER.info(f"ExtraSelect completed, data: {self.data}")
        except Exception as e:
            LOGGER.error(f"Error in _start_handler for {self.mode}: {e}", exc_info=True)
            self.is_cancel = True
            await self._cleanup()

    async def on_selection_complete(self):
        LOGGER.info(f"Selection complete for {self.mode}, proceeding with processing")

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update for {self.mode}: {status}")
        except Exception as e:
            LOGGER.error(f"Failed to send status: {e}", exc_info=True)

    async def _final_path(self, outfile=''):
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
                self._files.clear()
                LOGGER.info(f"Final path set to: {self._up_path}")
                return self._up_path
            except Exception as e:
                LOGGER.error(f"Final path error: {e}", exc_info=True)
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
                    LOGGER.info(f"Generated name with info: {file_name}")
                self.name = file_name
            if not self.name.upper().endswith(('MP4', 'MKV')):
                self.name += '.mkv'
            LOGGER.info(f"Set name: {self.name} with base_dir: {base_dir}")
            return base_dir if await aiopath.isfile(path) else path

    async def _run_cmd(self, cmd, status='prog'):
        try:
            await self._send_status(status)
            LOGGER.info(f"Running FFmpeg cmd: {' '.join(cmd)}")
            self.listener.suproc = await create_subprocess_exec(*cmd, stderr=PIPE)
            _, code = await gather(self.progress(status), self.listener.suproc.wait())
            if code == 0:
                LOGGER.info(f"FFmpeg succeeded for {self.mode}")
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancel = True
                LOGGER.info(f"FFmpeg cancelled for {self.mode}")
            else:
                error_msg = (await self.listener.suproc.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error: {error_msg}")
                self.is_cancel = True
            await self._cleanup()
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error: {e}", exc_info=True)
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
                    LOGGER.error("Failed to merge files.")
                    await self._cleanup()
                    return self._up_path
            except Exception as e:
                LOGGER.error(f"Error merging: {e}", exc_info=True)
                await self._cleanup()
                return self._up_path
            finally:
                if await aiopath.exists(input_file):
                    await clean_target(input_file)
                    LOGGER.info(f"Cleaned input file: {input_file}")

        streams, _ = await get_metavideo(self.outfile)
        if not streams:
            LOGGER.error(f"No streams in merged file: {self.outfile}")
            await self._cleanup()
            return self._up_path

        await self._start_handler(streams)
        if self.is_cancel or not self.data:
            LOGGER.warning(f"Cancelled or no data: is_cancel={self.is_cancel}, data={self.data}")
            await self._cleanup()
            return self.outfile

        streams_to_remove = self.data.get('streams_to_remove', [])
        if not streams_to_remove:
            LOGGER.info("No streams selected to remove.")
            return await self._final_path(self.outfile)

        final_outfile = ospath.join(base_dir, self.name)
        cmd = [FFMPEG_NAME, '-i', self.outfile]
        kept_streams = [f'0:{s["index"]}' for s in streams if s['index'] not in streams_to_remove and s['codec_type'] != 'video']
        cmd.extend(['-map'] + kept_streams if kept_streams else ['-map', '0:v'])
        cmd.extend(('-c', 'copy', final_outfile, '-y'))
        LOGGER.info(f"Executing FFmpeg remove cmd: {' '.join(cmd)}")
        if not await self._run_cmd(cmd, 'direct'):
            LOGGER.error("Failed to remove tracks.")
            await self._cleanup()
            return self.outfile

        if await aiopath.exists(self.outfile):
            await clean_target(self.outfile)
            LOGGER.info(f"Cleaned temp file: {self.outfile}")
        return await self._final_path(final_outfile)

    async def _merge_preremove_audio(self):
        file_list = await self._get_files()
        if not file_list:
            LOGGER.error("No valid video files found.")
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
        streams, _ = await get_metavideo(self.path)
        if not streams:
            LOGGER.error(f"No streams found in {self.path}")
            await self._cleanup()
            return self._up_path
        await self._start_handler(streams)
        if self.is_cancel or not self.data:
            LOGGER.warning(f"Cancelled or no data: is_cancel={self.is_cancel}, data={self.data}")
            await self._cleanup()
            return self._up_path

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