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
from bot.helper.ext_utils.bot_utils import sync_to_async, cmd_exec
from bot.helper.ext_utils.files_utils import get_path_size, clean_target
from bot.helper.ext_utils.links_utils import get_url_name
from bot.helper.ext_utils.media_utils import get_document_type, FFProgress
from bot.helper.listeners import tasks_listener as task
from bot.helper.mirror_utils.status_utils.ffmpeg_status import FFMpegStatus
from bot.helper.telegram_helper.message_utils import sendStatusMessage, sendMessage
from bot.helper.video_utils.extra_selector import ExtraSelect

file_lock = Lock()
io_lock = Lock()

async def get_metavideo(video_file):
    async with io_lock:
        try:
            stdout, stderr, rcode = await cmd_exec(['ffprobe', '-hide_banner', '-print_format', 'json', '-show_streams', video_file])
            if rcode != 0:
                LOGGER.error(f"ffprobe error: {stderr}")
                return []
            from ast import literal_eval
            metadata = literal_eval(stdout)
            return metadata.get('streams', [])
        except Exception as e:
            LOGGER.error(f"Error in get_metavideo: {e}")
            return []

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
        self._files = []
        super().__init__()
        self.is_cancelled = False
        LOGGER.info(f"Initialized VidEcxecutor for MID: {self.listener.mid}, path: {self.path}")

    async def _cleanup(self):
        async with file_lock:
            try:
                for f in self._files:
                    if await aiopath.exists(f):
                        await clean_target(f)
                self._files.clear()
                input_file = ospath.join(self.path, f'input_{self._gid}.txt')
                if await aiopath.exists(input_file):
                    await clean_target(input_file)
                if self.is_cancelled:
                    self.data.clear()
                LOGGER.info(f"Cleanup completed for {self.mode}")
            except Exception as e:
                LOGGER.error(f"Cleanup error: {e}")

    async def _extract_zip(self, zip_path):
        extract_dir = ospath.join(ospath.dirname(zip_path), f"extracted_{self._gid}")
        try:
            await makedirs(extract_dir, exist_ok=True)
            cmd = ['7z', 'x', zip_path, f'-o{extract_dir}', '-y']
            _, stderr, rcode = await cmd_exec(cmd)
            if rcode != 0:
                LOGGER.error(f"Failed to extract ZIP: {stderr}")
                await rmtree(extract_dir, ignore_errors=True)
                return None
            LOGGER.info(f"Extracted ZIP to {extract_dir}")
            return extract_dir
        except Exception as e:
            LOGGER.error(f"ZIP extraction error: {e}")
            await rmtree(extract_dir, ignore_errors=True)
            return None

    async def _get_files(self):
        file_list = []
        async with io_lock:
            if self._metadata:
                file_list.append(self.path)
            elif await aiopath.isfile(self.path) and self.path.lower().endswith('.zip'):
                extract_dir = await self._extract_zip(self.path)
                if extract_dir:
                    self._files.append(extract_dir)
                    for dirpath, _, files in await sync_to_async(walk, extract_dir):
                        for file in natsorted(files):
                            file_path = ospath.join(dirpath, file)
                            if (await get_document_type(file_path))[0]:
                                file_list.append(file_path)
                                LOGGER.info(f"Found media file: {file_path}")
                    if not file_list:
                        LOGGER.error(f"No valid media files in ZIP: {self.path}")
                    self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
            elif await aiopath.isfile(self.path):
                if (await get_document_type(self.path))[0]:
                    file_list.append(self.path)
                    self.size = await get_path_size(self.path)
            else:
                for dirpath, _, files in await sync_to_async(walk, self.path):
                    for file in natsorted(files):
                        file_path = ospath.join(dirpath, file)
                        if (await get_document_type(file_path))[0]:
                            file_list.append(file_path)
                            LOGGER.info(f"Found media file: {file_path}")
                    self.size = sum(await gather(*[get_path_size(f) for f in file_list])) if file_list else 0
            if not file_list:
                LOGGER.error(f"No valid files found for {self.mode}")
            return file_list

    async def execute(self):
        self._is_dir = await aiopath.isdir(self.path)
        try:
            self.mode, self.name, _ = self.listener.vidMode
        except AttributeError as e:
            LOGGER.error(f"Invalid vidMode: {e}")
            await self._cleanup()
            return self._up_path
        LOGGER.info(f"Executing {self.mode} with name: {self.name}")
        file_list = await self._get_files()
        if not file_list:
            await sendMessage("No video files found in ZIP. Sending original file.", self.listener.message)
            return self._up_path
        try:
            result = {
                'merge_rmaudio': self._merge_and_rmaudio,
                'merge_preremove_audio': self._merge_preremove_audio,
            }[self.mode](file_list)
            LOGGER.info(f"{self.mode} completed with result: {result}")
            return await result
        except Exception as e:
            LOGGER.error(f"Execution error in {self.mode}: {e}")
            await self._cleanup()
            return self._up_path
        finally:
            if self.is_cancelled:
                await self._cleanup()

    async def _start_handler(self, *args):
        LOGGER.info(f"Starting handler for {self.mode}")
        await sleep(1)
        try:
            selector = ExtraSelect(self)
            await selector.get_buttons(*args)
        except Exception as e:
            LOGGER.error(f"Error in _start_handler: {e}")
            self.is_cancelled = True
            await self._cleanup()

    async def process_selections(self):
        LOGGER.info(f"Processing selections for {self.mode}")

    async def _send_status(self, status='wait'):
        try:
            async with task_dict_lock:
                task_dict[self.listener.mid] = FFMpegStatus(self.listener, self, self._gid, status)
            await sendStatusMessage(self.listener.message)
            LOGGER.info(f"Sent status update: {status}")
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
                            if file != ospath.basename(outfile or self.outfile):
                                await clean_target(ospath.join(dirpath, file))
                    all_files = [(dirpath, file) for dirpath, _, files in await sync_to_async(walk, scan_dir) for file in files]
                    if len(all_files) == 1:
                        self._up_path = ospath.join(*all_files[0])
                self._files.clear()
                LOGGER.info(f"Final path: {self._up_path}")
                return self._up_path
            except Exception as e:
                LOGGER.error(f"Final path error: {e}")
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
                    LOGGER.info(f"Generated name: {file_name}")
                self.name = file_name
            if not self.name.upper().endswith(('MKV')):
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
                LOGGER.info(f"FFmpeg succeeded")
                return True
            if self.listener.suproc == 'cancelled' or code == -9:
                self.is_cancelled = True
                LOGGER.info(f"FFmpeg cancelled")
            else:
                error_msg = (await self.listener.suproc.stderr.read()).decode().strip()
                LOGGER.error(f"FFmpeg error: {error_msg}")
                self.is_cancelled = True
            await self._cleanup()
            return False
        except Exception as e:
            LOGGER.error(f"Run cmd error: {e}")
            self.is_cancelled = True
            await self._cleanup()
            return False

    async def _check_compatibility(self, file_list):
        stream_data = await gather(*[get_metavideo(f) for f in file_list])
        video_streams = [s for streams in stream_data for s in streams if s['codec_type'] == 'video']
        if not video_streams:
            LOGGER.error("No video streams found in files.")
            return False
        base_codec = video_streams[0]['codec_name']
        base_res = (video_streams[0].get('width', 0), video_streams[0].get('height', 0))
        for vs in video_streams[1:]:
            if vs['codec_name'] != base_codec or (vs.get('width', 0), vs.get('height', 0)) != base_res:
                LOGGER.warning(f"Video incompatibility detected: {vs['codec_name']} {vs.get('width', 0)}x{vs.get('height', 0)} vs {base_codec} {base_res[0]}x{base_res[1]}")
                return False
        return True

    async def _merge_and_rmaudio(self, file_list):
        base_dir = await self._name_base_dir(file_list[0], 'Merge-RemoveAudio', True)
        
        # Step 0: Check compatibility
        if not await self._check_compatibility(file_list):
            LOGGER.error("Files are not compatible for concatenation.")
            await sendMessage("Video files are incompatible (different codecs or resolutions). Sending original ZIP.", self.listener.message)
            return self._up_path

        # Step 1: Merge all files into a temporary file with all streams
        temp_outfile = ospath.join(base_dir, f"temp_merge_{self._gid}.mkv")
        self._files = file_list
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
        try:
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join([f"file '{f}'" for f in file_list]))
            LOGGER.info(f"Created input file: {input_file}")
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', '-map', '0', temp_outfile, '-y']
            if not await self._run_cmd(cmd, 'direct'):
                LOGGER.error("Failed to merge files.")
                await sendMessage("Merging failed. Sending original ZIP.", self.listener.message)
                return self._up_path
        except Exception as e:
            LOGGER.error(f"Error merging: {e}")
            await sendMessage("Merging failed. Sending original ZIP.", self.listener.message)
            return self._up_path
        finally:
            if await aiopath.exists(input_file):
                await clean_target(input_file)

        # Step 2: Get streams from merged file and allow selection
        streams = await get_metavideo(temp_outfile)
        if not streams:
            LOGGER.error(f"No streams in merged file: {temp_outfile}")
            await sendMessage("No streams found in merged file. Sending original ZIP.", self.listener.message)
            await clean_target(temp_outfile)
            return self._up_path

        # Step 3: Present streams for removal (non-video only)
        await self._start_handler(streams)
        if self.is_cancelled or not self.data:
            LOGGER.warning(f"Cancelled or no data: {self.data}")
            await sendMessage("Selection cancelled or failed. Sending original ZIP.", self.listener.message)
            await clean_target(temp_outfile)
            return self._up_path

        # Step 4: Process selections and create final output
        streams_to_remove = self.data.get('streams_to_remove', [])
        self.outfile = ospath.join(base_dir, self.name)
        cmd = [FFMPEG_NAME, '-i', temp_outfile, '-map', '0:v']  # Keep all video streams
        if not streams_to_remove:
            LOGGER.info("No streams selected to remove, keeping all tracks.")
            cmd.extend(['-map', '0:a?', '-map', '0:s?', '-c', 'copy', self.outfile, '-y'])
        else:
            # Only allow removal of non-video streams
            kept_streams = [f'0:{s["index"]}' for s in streams if s['codec_type'] != 'video' and s['index'] not in streams_to_remove]
            cmd.extend(['-map'] + kept_streams if kept_streams else [])
            cmd.extend(('-c', 'copy', self.outfile, '-y'))
        LOGGER.info(f"Executing FFmpeg remove cmd: {' '.join(cmd)}")
        if not await self._run_cmd(cmd, 'direct'):
            LOGGER.error("Failed to process merged file.")
            await sendMessage("Processing failed. Sending original ZIP.", self.listener.message)
            await clean_target(temp_outfile)
            return self._up_path

        if await aiopath.exists(temp_outfile):
            await clean_target(temp_outfile)
        return await self._final_path()

    async def _merge_preremove_audio(self, file_list):
        base_dir = await self._name_base_dir(file_list[0], 'Merge-PreRemoveAudio', True)
        stream_data = await gather(*[get_metavideo(f) for f in file_list])
        streams_per_file = {f: streams for f, streams in zip(file_list, stream_data) if streams}

        temp_files = []
        for file in file_list:
            if file not in streams_per_file:
                LOGGER.warning(f"No streams in {file}, skipping.")
                continue
            await self._start_handler(streams_per_file[file])
            if self.is_cancelled or not self.data:
                LOGGER.warning(f"Cancelled or no data for {file}: {self.data}")
                await sendMessage("Selection cancelled or failed. Sending original ZIP.", self.listener.message)
                return self._up_path

            outfile = ospath.join(base_dir, f"temp_{ospath.basename(file)}_{self._gid}")
            cmd = [FFMPEG_NAME, '-i', file, '-map', '0:v']  # Keep all video streams
            selections = self.data.get('streams_to_remove', [])
            streams = streams_per_file[file]
            if selections:
                kept_streams = [f'0:{s["index"]}' for s in streams if s['codec_type'] != 'video' and f"{file}_{s['index']}" not in selections]
                cmd.extend(['-map'] + kept_streams if kept_streams else [])
            else:
                cmd.extend(('-map', '0', '-c', 'copy', outfile, '-y'))
            cmd.extend(('-c', 'copy', outfile, '-y'))
            if await self._run_cmd(cmd):
                temp_files.append(outfile)
                LOGGER.info(f"Processed file {file} to {outfile}")
            else:
                LOGGER.error(f"Failed to process {file}.")
                await gather(*[clean_target(f) for f in temp_files if await aiopath.exists(f)])
                await sendMessage("Processing failed. Sending original ZIP.", self.listener.message)
                return self._up_path
            self.data.clear()

        if not temp_files:
            LOGGER.error("No files processed successfully.")
            await sendMessage("No files processed. Sending original ZIP.", self.listener.message)
            return self._up_path

        self.outfile = ospath.join(base_dir, self.name)
        self._files = temp_files
        input_file = ospath.join(base_dir, f'input_{self._gid}.txt')
        try:
            async with aiopen(input_file, 'w') as f:
                await f.write('\n'.join([f"file '{f}'" for f in temp_files]))
            cmd = [FFMPEG_NAME, '-f', 'concat', '-safe', '0', '-i', input_file, '-c', 'copy', self.outfile, '-y']
            if not await self._run_cmd(cmd, 'direct'):
                LOGGER.error("Failed to merge files.")
                await sendMessage("Merging failed. Sending original ZIP.", self.listener.message)
                return self._up_path
            if not self.is_cancelled:
                await gather(*[clean_target(f) for f in temp_files if await aiopath.exists(f)])
            return await self._final_path()
        except Exception as e:
            LOGGER.error(f"Error merging: {e}")
            await sendMessage("Merging failed. Sending original ZIP.", self.listener.message)
            return self._up_path
        finally:
            if await aiopath.exists(input_file):
                await clean_target(input_file)

    async def _rm_audio_single(self):
        streams = await get_metavideo(self.path)
        if not streams:
            LOGGER.error(f"No streams found in {self.path}")
            await sendMessage("No streams found. Sending original file.", self.listener.message)
            return self._up_path
        await self._start_handler(streams)
        if self.is_cancelled or not self.data:
            LOGGER.warning(f"Cancelled or no data: {self.data}")
            await sendMessage("Selection cancelled or failed. Sending original file.", self.listener.message)
            return self._up_path

        base_dir = await self._name_base_dir(self.path, 'RemoveAudio')
        self.outfile = ospath.join(base_dir, self.name)
        cmd = [FFMPEG_NAME, '-i', self.path, '-map', '0:v']  # Keep all video streams
        selections = self.data.get('streams_to_remove', [])
        if selections:
            kept_streams = [f'0:{s["index"]}' for s in streams if s['codec_type'] != 'video' and s['index'] not in selections]
            cmd.extend(['-map'] + kept_streams if kept_streams else [])
        else:
            cmd.extend(('-map', '0', '-c', 'copy', self.outfile, '-y'))
        cmd.extend(('-c', 'copy', self.outfile, '-y'))
        if not await self._run_cmd(cmd):
            LOGGER.error("Failed to process file.")
            await sendMessage("Processing failed. Sending original file.", self.listener.message)
            return self._up_path
        return await self._final_path()