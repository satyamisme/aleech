from __future__ import annotations
from ast import literal_eval
from asyncio import Event, wait_for, wrap_future, gather, Lock
from functools import partial
from pyrogram.filters import regex, user
from pyrogram.handlers import CallbackQueryHandler
from pyrogram.types import CallbackQuery
from time import time
import os
import asyncio

from bot import LOGGER, VID_MODE
from bot.helper.ext_utils.bot_utils import new_thread
from bot.helper.ext_utils.status_utils import get_readable_file_size, get_readable_time
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, deleteMessage
from bot.helper.video_utils import executor as exc

data_lock = Lock()

class ExtraSelect:
    def __init__(self, executor: exc.VidEcxecutor):
        self._listener = executor.listener
        self._time = time()
        self._reply = None
        self.executor = executor
        self.event = Event()
        self.is_cancel = False
        self.extension = [None, None, 'mkv']
        self.status = ''
        self.stream_page = {}

    @new_thread
    async def _event_handler(self):
        LOGGER.info(f"Starting ExtraSelect event handler for {self.executor.mode}")
        pfunc = partial(cb_extra, obj=self)
        handler = self._listener.client.add_handler(CallbackQueryHandler(pfunc, filters=regex('^extra') & user(self._listener.user_id)), group=-1)
        try:
            await wait_for(self.event.wait(), timeout=180)
            LOGGER.info(f"ExtraSelect event completed successfully for {self.executor.mode}")
        except TimeoutError:
            LOGGER.warning(f"ExtraSelect timed out for {self.executor.mode}")
            self.is_cancel = True
            await self._cleanup()
        except Exception as e:
            LOGGER.error(f"Event handler error in ExtraSelect: {str(e)}", exc_info=True)
            self.is_cancel = True
            await self._cleanup()
        finally:
            self._listener.client.remove_handler(*handler)
            self.event.set()

    async def _cleanup(self):
        async with data_lock:
            LOGGER.info(f"Cleaning up ExtraSelect for {self.executor.mode}")
            self.executor.data.clear()
            self.executor.is_cancel = True
            self.executor.event.set()

    async def update_message(self, text: str, buttons):
        try:
            if not self._reply:
                LOGGER.info(f"Sending initial ExtraSelect message for {self.executor.mode}")
                self._reply = await sendMessage(text, self._listener.message, buttons)
            else:
                LOGGER.info(f"Updating ExtraSelect message for {self.executor.mode}")
                await editMessage(text, self._reply, buttons)
            if not self._reply:
                LOGGER.error(f"Failed to send/update message for {self.executor.mode}")
        except Exception as e:
            LOGGER.error(f"Failed to update message: {e}")

    def _format_stream_name(self, stream):
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height', '') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    async def streams_select(self, streams, mode=None):
        async with data_lock:
            if not streams:
                LOGGER.warning(f"No streams provided for {mode}")
                return "No streams available.", ButtonMaker().build_menu(1)
            buttons = ButtonMaker()
            if not self.executor.data:
                LOGGER.info(f"Initializing stream data for {mode}")
                self.executor.data = {'streams': {}, 'streams_to_remove': [], 'sdata': []}
                if isinstance(streams, tuple):
                    streams, _ = streams
                if isinstance(streams, list):
                    for stream in streams:
                        if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                            index = stream['index']
                            self.executor.data['streams'][index] = stream
                            stream_info = self._format_stream_name(stream)
                            self.executor.data['streams'][index]['info'] = stream_info
                elif isinstance(streams, dict):
                    for file, file_streams in streams.items():
                        for stream in file_streams:
                            if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                                index = stream['index']
                                key = f"{file}_{index}"
                                self.executor.data['streams'][key] = stream
                                self.executor.data['streams'][key]['info'] = self._format_stream_name(stream)
                else:
                    LOGGER.warning(f"Unexpected streams type for mode {mode}: {type(streams)}")
                    return "No streams available.", buttons.build_menu(1)

            mode, ddict = self.executor.mode, self.executor.data
            streams_dict = ddict['streams']
            self.stream_page.setdefault(mode, 0)
            streams_per_page = 5
            total_streams = len(streams_dict)
            total_pages = max(1, (total_streams + streams_per_page - 1) // streams_per_page)
            page = min(self.stream_page[mode], total_pages - 1)
            start_idx = min(page * streams_per_page, total_streams)
            end_idx = min(start_idx + streams_per_page, total_streams)
            displayed_streams = list(streams_dict.items())[start_idx:end_idx]

            text = (f'<b>{VID_MODE[mode].upper()} ~ {self._listener.tag}</b>\n'
                    f'<code>{self.executor.name}</code>\n'
                    f'Size: <b>{get_readable_file_size(self.executor.size)}</b>\n')

            if displayed_streams:
                text += '\n<b>Streams:</b>\n'
                for i, (key, value) in enumerate(displayed_streams, start=start_idx + 1):
                    is_selected = key in ddict.get('streams_to_remove', []) or key in ddict.get('sdata', [])
                    value['info'] = f"🔵 {value['info'].replace('🔵 ', '')}" if is_selected else value['info'].replace('🔵 ', '')
                    text += f"{i}. {value['info']}\n"
                    buttons.button_data(f"{i}", f'extra {mode} {key}', 'footer')
            else:
                text += '\nNo streams available.'

            if mode in ['merge_rmaudio', 'merge_preremove_audio', 'rmstream']:
                if mode == 'rmstream' and ddict.get('sdata'):
                    text += '\n<b>To Remove:</b>\n'
                    for i, sindex in enumerate(ddict['sdata'], start=1):
                        text += f"{i}. {ddict['streams'][sindex]['info'].replace('🔵 ', '')}\n"
                elif ddict.get('streams_to_remove'):
                    text += '\n<b>To Remove:</b>\n'
                    for i, stream_key in enumerate(ddict['streams_to_remove'], start=1):
                        text += f"{i}. {ddict['streams'][stream_key]['info'].replace('🔵 ', '')}\n"
                buttons.button_data('All', f'extra {mode} all', 'footer')
                buttons.button_data('Reset', f'extra {mode} reset', 'header')
                buttons.button_data('Reverse', f'extra {mode} reverse', 'header')
                if mode == 'rmstream':
                    buttons.button_data('All Audio', f'extra {mode} audio', 'footer')
                    buttons.button_data('All Subs', f'extra {mode} subtitle', 'footer')
                has_selections = bool(ddict.get('streams_to_remove', []) or ddict.get('sdata', []))
                buttons.button_data('Continue' if not has_selections else 'Continue (Remove)', f'extra {mode} continue', 'footer')

            buttons.button_data('Cancel', 'extra cancel', 'footer')
            if total_streams > streams_per_page:
                if page > 0:
                    buttons.button_data('Prev', f'extra {mode} prev', 'footer')
                if page < total_pages - 1:
                    buttons.button_data('Next', f'extra {mode} next', 'footer')

            text += f'\n<i>Time Left: {get_readable_time(180 - (time() - self._time))}</i>'
            LOGGER.info(f"Prepared streams_select text for {mode}: {text}")
            return text, buttons.build_menu(2)

    async def merge_rmaudio_select(self, streams):
        if isinstance(streams, tuple):
            streams, _ = streams
        text, buttons = await self.streams_select(streams, 'merge_rmaudio')
        await self.update_message(text, buttons)

    async def merge_preremove_audio_select(self, streams_per_file: dict):
        text, buttons = await self.streams_select(streams_per_file, 'merge_preremove_audio')
        await self.update_message(text, buttons)

    async def compress_select(self, streams: dict):
        if isinstance(streams, tuple):
            streams, _ = streams
        async with data_lock:
            self.executor.data = {}
            buttons = ButtonMaker()
            for stream in streams:
                index, codec_type, lang = stream.get('index'), stream.get('codec_type'), stream.get('tags', {}).get('language', 'Unknown')
                if codec_type == 'video' and 'video' not in self.executor.data:
                    self.executor.data['video'] = index
                if codec_type == 'audio':
                    buttons.button_data(f'Audio ~ {lang.upper()}', f'extra compress {index}', 'footer')
            buttons.button_data('Continue', 'extra compress 0', 'footer')
            buttons.button_data('Cancel', 'extra cancel', 'footer')
            await self.update_message(f'{self._listener.tag}, Select audio or press <b>Continue (no audio)</b>.\n<code>{self.executor.name}</code>', buttons.build_menu(2))

    async def rmstream_select(self, streams):
        if isinstance(streams, tuple):
            streams, _ = streams
        text, buttons = await self.streams_select(streams, 'rmstream')
        await self.update_message(text, buttons)

    async def convert_select(self, streams: dict):
        if isinstance(streams, tuple):
            streams, _ = streams
        async with data_lock:
            buttons = ButtonMaker()
            hvid = '1080p'
            resolution = {'1080p': 'Convert 1080p', '720p': 'Convert 720p', '540p': 'Convert 540p', '480p': 'Convert 480p', '360p': 'Convert 360p'}
            for stream in streams:
                if stream['codec_type'] == 'video':
                    vid_height = f'{stream["height"]}p'
                    if vid_height in resolution:
                        hvid = vid_height
                    break
            keys = list(resolution)
            for key in keys[keys.index(hvid)+1:]:
                buttons.button_data(resolution[key], f'extra convert {key}', 'footer')
            buttons.button_data('Cancel', 'extra cancel', 'footer')
            await self.update_message(f'{self._listener.tag}, Select resolution to convert.\n<code>{self.executor.name}</code>', buttons.build_menu(2))

    async def subsync_select(self):
        buttons = ButtonMaker()
        text = ''
        if not self.status:
            async with data_lock:
                self.executor.data['list'] = {}
                for position, file in enumerate(sorted(await listdir(self.executor.path)), 1):
                    file_path = os.path.join(self.executor.path, file)
                    if (await exc.get_document_type(file_path))[0] or file.endswith(('.srt', '.ass')):
                        self.executor.data['list'][position] = file
                if not self.executor.data['list']:
                    await self._cleanup()
                    return self.executor._up_path
                self.executor.data['final'] = {}
                self._start_handler()
                await gather(self._send_status(), wait_for(self.event.wait(), timeout=300))
                if self.is_cancel or not self.executor.data.get('final'):
                    await self._cleanup()
                    return self.executor._up_path
                return self.executor._up_path
        else:
            file = self.executor.data['list'][self.status]
            text = f'Current: <b>{file}</b>\n'
            if ref := self.executor.data['final'].get(self.status, {}).get('ref'):
                text += f'References: <b>{ref}</b>\n'
            text += '\nSelect Available References Below!\n'
            index = 1
            for position, file in self.executor.data['list'].items():
                if position != self.status and file not in [v['file'] for v in self.executor.data['final'].values()]:
                    text += f'{index}. {file}\n'
                    buttons.button_data(str(index), f'extra subsync select {position}', 'footer')
                    index += 1
            await self.update_message(text, buttons.build_menu(5))

    async def extract_select(self, streams: dict):
        if isinstance(streams, tuple):
            streams, _ = streams
        async with data_lock:
            self.executor.data = {}
            ext = [None, None, 'mkv']
            for stream in streams:
                codec_name, codec_type = stream.get('codec_name'), stream.get('codec_type')
                if codec_type == 'audio' and not ext[0]:
                    ext[0] = 'aac' if codec_name in ['aac', 'mp3'] else 'mkv'
                elif codec_type == 'subtitle' and not ext[1]:
                    ext[1] = 'srt' if codec_name == 'subrip' else 'ass'
            self.extension = ext
            text, buttons = await self.streams_select(streams, 'extract')
            await self.update_message(text, buttons)

    async def get_buttons(self, *args):
        LOGGER.info(f"Starting get_buttons for mode {self.executor.mode} with args: {args}")
        future = self._event_handler()
        try:
            if not args or not args[0]:
                LOGGER.error(f"No valid streams passed to get_buttons for {self.executor.mode}")
                return
            if self.executor.mode == 'merge_rmaudio':
                await self.merge_rmaudio_select(*args)
            elif self.executor.mode == 'merge_preremove_audio':
                await self.merge_preremove_audio_select(*args)
            elif self.executor.mode == 'compress':
                await self.compress_select(*args)
            elif self.executor.mode == 'rmstream':
                await self.rmstream_select(*args)
            elif self.executor.mode == 'convert':
                await self.convert_select(*args)
            elif self.executor.mode == 'subsync':
                await self.subsync_select()
            elif self.executor.mode == 'extract':
                await self.extract_select(*args)
            await wrap_future(future)
            LOGGER.info(f"get_buttons finished awaiting event for {self.executor.mode}, data: {self.executor.data}")
        except Exception as e:
            LOGGER.error(f"Error in get_buttons: {e}", exc_info=True)
            await self._cleanup()
        finally:
            if self._reply:
                await deleteMessage(self._reply)
            self.executor.event.set()
            if self.is_cancel:
                self._listener.suproc = 'cancelled'
                await self._listener.onUploadError(f'{VID_MODE[self.executor.mode]} stopped by user!')
            LOGGER.info(f"get_buttons completed for {self.executor.mode}, is_cancel: {self.is_cancel}, data: {self.executor.data}")

async def cb_extra(_, query: CallbackQuery, obj: ExtraSelect):
    data = query.data.split()
    if len(data) < 2:
        await query.answer("Invalid callback data!", show_alert=True)
        return
    mode = data[1]
    await query.answer()
    LOGGER.info(f"Received callback: {query.data}")

    async with data_lock:
        if data[2] == 'cancel':
            LOGGER.info(f"Cancel triggered for {mode}")
            obj.is_cancel = obj.executor.is_cancel = True
            await obj._cleanup()
            obj.event.set()
        elif data[2] in ['prev', 'next']:
            obj.stream_page[mode] = max(0, obj.stream_page.get(mode, 0) + (1 if data[2] == 'next' else -1))
            LOGGER.info(f"Page navigation: {data[2]} for {mode}, new page: {obj.stream_page[mode]}")
            if mode == 'merge_rmaudio':
                await obj.merge_rmaudio_select(None)
            elif mode == 'merge_preremove_audio':
                await obj.merge_preremove_audio_select(obj.executor.data.get('streams_per_file', {}))
            elif mode == 'rmstream':
                await obj.rmstream_select(obj.executor.data.get('streams', []))
            elif mode == 'extract':
                await obj.extract_select(obj.executor.data.get('streams', []))
        elif data[2] == 'reset':
            LOGGER.info(f"Resetting selections for {mode}")
            for index in obj.executor.data.get('streams_to_remove', []) + obj.executor.data.get('sdata', []):
                obj.executor.data['streams'][index]['info'] = obj.executor.data['streams'][index]['info'].replace('🔵 ', '')
            obj.executor.data['streams_to_remove'] = []
            obj.executor.data['sdata'] = []
            await obj.update_message(*(await obj.streams_select(None, mode)))
        elif data[2] == 'continue':
            LOGGER.info(f"Continue triggered for {mode}, setting event with data: {obj.executor.data}")
            obj.event.set()
            await asyncio.sleep(1)
            LOGGER.info(f"Event set for {mode}, selections: {obj.executor.data.get('streams_to_remove', [])}")
        elif mode == 'merge_rmaudio':
            if data[2] == 'all':
                LOGGER.info(f"Selecting all streams to remove for {mode}")
                obj.executor.data['streams_to_remove'] = list(obj.executor.data['streams'].keys())
                for index in obj.executor.data['streams_to_remove']:
                    obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info'].replace('🔵 ', '')}"
                await obj.merge_rmaudio_select(None)
            elif data[2] == 'reverse':
                LOGGER.info(f"Reversing selections for {mode}")
                all_streams = list(obj.executor.data['streams'].keys())
                obj.executor.data['streams_to_remove'] = [s for s in all_streams if s not in obj.executor.data['streams_to_remove']]
                for index in all_streams:
                    obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info'].replace('🔵 ', '')}" if index in obj.executor.data['streams_to_remove'] else obj.executor.data['streams'][index]['info'].replace('🔵 ', '')
                await obj.merge_rmaudio_select(None)
            else:
                try:
                    stream_key = int(data[2])
                    streams_to_remove = obj.executor.data.get('streams_to_remove', [])
                    if stream_key in streams_to_remove:
                        streams_to_remove.remove(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '')
                        LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                    else:
                        streams_to_remove.append(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = f"🔵 {obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '')}"
                        LOGGER.info(f"Selected stream {stream_key} for {mode}")
                    obj.executor.data['streams_to_remove'] = streams_to_remove
                    await obj.merge_rmaudio_select(None)
                except (ValueError, KeyError) as e:
                    LOGGER.error(f"Invalid stream key or data error: {e}")
                    await obj.update_message(f"Error selecting stream {data[2]}. Please try again.", buttons.build_menu(2))
        elif mode == 'merge_preremove_audio':
            if data[2] == 'all':
                LOGGER.info(f"Selecting all streams to remove for {mode}")
                obj.executor.data['streams_to_remove'] = list(obj.executor.data['streams'].keys())
                for index in obj.executor.data['streams_to_remove']:
                    obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info'].replace('🔵 ', '')}"
                await obj.merge_preremove_audio_select(obj.executor.data.get('streams_per_file', {}))
            elif data[2] == 'reverse':
                LOGGER.info(f"Reversing selections for {mode}")
                all_streams = list(obj.executor.data['streams'].keys())
                obj.executor.data['streams_to_remove'] = [s for s in all_streams if s not in obj.executor.data['streams_to_remove']]
                for index in all_streams:
                    obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info'].replace('🔵 ', '')}" if index in obj.executor.data['streams_to_remove'] else obj.executor.data['streams'][index]['info'].replace('🔵 ', '')
                await obj.merge_preremove_audio_select(obj.executor.data.get('streams_per_file', {}))
            else:
                try:
                    stream_key = data[2]
                    streams_to_remove = obj.executor.data.get('streams_to_remove', [])
                    if stream_key in streams_to_remove:
                        streams_to_remove.remove(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '')
                        LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                    else:
                        streams_to_remove.append(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = f"🔵 {obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '')}"
                        LOGGER.info(f"Selected stream {stream_key} for {mode}")
                    obj.executor.data['streams_to_remove'] = streams_to_remove
                    await obj.merge_preremove_audio_select(obj.executor.data.get('streams_per_file', {}))
                except (ValueError, KeyError) as e:
                    LOGGER.error(f"Invalid stream key or data error: {e}")
                    await obj.update_message(f"Error selecting stream {data[2]}. Please try again.", buttons.build_menu(2))
        elif mode == 'rmstream':
            if data[2] == 'all':
                if 'audio' in data[3].lower():
                    LOGGER.info(f"Selecting all audio streams for {mode}")
                    obj.executor.data['sdata'] = [s['index'] for s in obj.executor.data['streams'].values() if s['codec_type'] == 'audio']
                elif 'subtitle' in data[3].lower():
                    LOGGER.info(f"Selecting all subtitle streams for {mode}")
                    obj.executor.data['sdata'] = [s['index'] for s in obj.executor.data['streams'].values() if s['codec_type'] == 'subtitle']
                for index in obj.executor.data['sdata']:
                    obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info'].replace('🔵 ', '')}"
                await obj.rmstream_select(obj.executor.data['streams'])
            elif data[2] == 'reverse':
                LOGGER.info(f"Reversing selections for {mode}")
                all_streams = list(obj.executor.data['streams'].keys())
                obj.executor.data['sdata'] = [s for s in all_streams if s not in obj.executor.data.get('sdata', [])]
                for index in all_streams:
                    obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info'].replace('🔵 ', '')}" if index in obj.executor.data['sdata'] else obj.executor.data['streams'][index]['info'].replace('🔵 ', '')
                await obj.rmstream_select(obj.executor.data['streams'])
            else:
                try:
                    stream_index = int(data[2])
                    sdata = obj.executor.data.get('sdata', [])
                    if stream_index in sdata:
                        sdata.remove(stream_index)
                        obj.executor.data['streams'][stream_index]['info'] = obj.executor.data['streams'][stream_index]['info'].replace('🔵 ', '')
                        LOGGER.info(f"Deselected stream {stream_index} for {mode}")
                    else:
                        sdata.append(stream_index)
                        obj.executor.data['streams'][stream_index]['info'] = f"🔵 {obj.executor.data['streams'][stream_index]['info'].replace('🔵 ', '')}"
                        LOGGER.info(f"Selected stream {stream_index} for {mode}")
                    obj.executor.data['sdata'] = sdata
                    await obj.rmstream_select(obj.executor.data['streams'])
                except (ValueError, KeyError) as e:
                    LOGGER.error(f"Invalid stream index or data error: {e}")
                    await obj.update_message(f"Error selecting stream {data[2]}. Please try again.", buttons.build_menu(2))
        elif mode == 'extract':
            if data[2] == 'alt':
                obj.executor.data['alt_mode'] = not literal_eval(data[3])
                LOGGER.info(f"Toggled ALT mode to {obj.executor.data['alt_mode']} for {mode}")
                await obj.extract_select(obj.executor.data.get('streams', []))
            elif data[2] == 'extension':
                ext_idx = {'video': 2, 'audio': 0, 'subtitle': 1}
                current_ext = obj.extension[ext_idx.get(data[3], 2)]
                obj.extension[ext_idx.get(data[3], 2)] = None if current_ext else data[3] if data[3] != 'default' else None
                LOGGER.info(f"Updated extension for {data[3]} to {obj.extension[ext_idx.get(data[3], 2)]}")
                await obj.extract_select(obj.executor.data.get('streams', []))
            elif data[2] in ['video', 'audio', 'subtitle']:
                obj.executor.data['key'] = data[2:]
                LOGGER.info(f"Extract all selected: {data[2:]}")
                obj.event.set()
            else:
                try:
                    stream_key = int(data[2])
                    obj.executor.data['key'] = stream_key
                    LOGGER.info(f"Extract single stream selected: {stream_key}")
                    obj.event.set()
                except ValueError:
                    LOGGER.error(f"Invalid stream key for extract: {data[2]}")
        elif mode == 'compress':
            if data[2] != 'cancel':
                obj.executor.data['audio'] = int(data[2])
                LOGGER.info(f"Compress audio selected: {data[2]}")
                obj.event.set()
        elif mode == 'convert':
            if data[2] != 'cancel':
                obj.executor.data = data[2]
                LOGGER.info(f"Convert resolution selected: {data[2]}")
                obj.event.set()
        elif mode == 'subsync':
            if data[2] == 'select':
                obj.executor.data['final'][obj.status] = {'file': obj.executor.data['list'][obj.status], 'ref': obj.executor.data['list'][int(data[3])]}
                LOGGER.info(f"Subsync reference selected: {obj.executor.data['list'][int(data[3])]} for {obj.executor.data['list'][obj.status]}")
                obj.status = ''
                await obj.subsync_select()
            elif not obj.status and data[2].isdigit():
                obj.status = int(data[2])
                LOGGER.info(f"Subsync status set to: {obj.status}")
                await obj.subsync_select()
            elif obj.status and data[2] == 'continue':
                LOGGER.info(f"Subsync continue triggered")
                obj.event.set()