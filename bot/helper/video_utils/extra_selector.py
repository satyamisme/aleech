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
        self.extension = [None, None, 'mkv']  # Default extensions: audio, subtitle, video
        self.status = ''
        self.stream_page = {}
        self.current_file_index = 0

    @new_thread
    async def _event_handler(self):
        """Manages callback events for stream selection with timeout and cleanup."""
        LOGGER.info(f"Starting ExtraSelect event handler for {self.executor.mode} (MID: {self.executor.listener.mid})")
        pfunc = partial(cb_extra, obj=self)
        handler = None
        try:
            handler = self._listener.client.add_handler(
                CallbackQueryHandler(pfunc, filters=regex('^extra') & user(self._listener.user_id)), group=-1)
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
            if handler:
                self._listener.client.remove_handler(*handler)
            self.event.set()
            LOGGER.info(f"Event handler finished for {self.executor.mode}, event set")

    async def _cleanup(self):
        """Cleans up resources and resets the executor state."""
        async with data_lock:
            LOGGER.info(f"Cleaning up ExtraSelect for {self.executor.mode}")
            if self.is_cancel:  # Only clear data if explicitly cancelled
                self.executor.data.clear()
            self.executor.is_cancel = True
            self.executor.event.set()
            if self._reply:
                await deleteMessage(self._reply)
                self._reply = None

    async def update_message(self, text: str, buttons):
        """Sends or updates the Telegram message with stream selection UI."""
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
            LOGGER.error(f"Failed to update message: {e}", exc_info=True)

    def _format_stream_name(self, stream):
        """Formats stream details for display in the UI."""
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    async def streams_select(self, streams=None, mode=None):
        """Generates a dynamic stream selection UI with pagination and file navigation."""
        async with data_lock:
            buttons = ButtonMaker()
            if not self.executor.data:
                if not streams:
                    LOGGER.warning(f"No streams provided for {mode}")
                    return "No streams available.", buttons.build_menu(1)
                LOGGER.info(f"Initializing stream data for {mode}")
                self.executor.data = {'streams': {}, 'streams_to_remove': [], 'sdata': []}
                if isinstance(streams, tuple):
                    streams, _ = streams
                if isinstance(streams, list):
                    for stream in streams:
                        if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                            index = stream['index']
                            self.executor.data['streams'][index] = stream
                            self.executor.data['streams'][index]['info'] = self._format_stream_name(stream)
                elif isinstance(streams, dict):
                    self.executor.data['sorted_files'] = sorted(streams.keys())
                    self.current_file_index = 0
                    for file in self.executor.data['sorted_files']:
                        for stream in streams[file]:
                            if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                                index = stream['index']
                                key = f"{file}_{index}"
                                self.executor.data['streams'][key] = stream
                                self.executor.data['streams'][key]['info'] = self._format_stream_name(stream)
                else:
                    LOGGER.warning(f"Unexpected streams type for mode {mode}: {type(streams)}")
                    return "No streams available.", buttons.build_menu(1)

            mode = mode or self.executor.mode
            if mode == 'merge_preremove_audio' and 'sorted_files' in self.executor.data:
                sorted_files = self.executor.data['sorted_files']
                self.current_file_index = max(0, min(self.current_file_index, len(sorted_files) - 1))
                current_file = sorted_files[self.current_file_index]
                current_streams = [
                    stream for key, stream in self.executor.data['streams'].items() if key.startswith(f"{current_file}_")
                ]
                text = (f'<b>{VID_MODE[mode].upper()} ~ {self._listener.tag}</b>\n'
                        f'<code>{current_file}</code>\n'
                        f'Size: <b>{get_readable_file_size(self.executor.size)}</b>\n'
                        f'\n<b>Streams for {current_file}:</b>\n')
                for i, stream in enumerate(current_streams, start=1):
                    key = f"{current_file}_{stream['index']}"
                    is_selected = key in self.executor.data['streams_to_remove']
                    LOGGER.info(f"Checking if {key} is selected: {is_selected}, streams_to_remove: {self.executor.data['streams_to_remove']}")
                    info = f"🔵 {stream['info'].replace('🔵 ', '').replace('🔴 ', '')}" if is_selected else stream['info'].replace('🔵 ', '').replace('🔴 ', '')
                    text += f"{i}. {info}\n"
                    buttons.button_data(f"{i}", f'extra {mode} {key}', 'footer')
                if self.current_file_index > 0:
                    buttons.button_data('Previous File', f'extra {mode} prev_file', 'footer')
                if self.current_file_index < len(sorted_files) - 1:
                    buttons.button_data('Next File', f'extra {mode} next_file', 'footer')
                buttons.button_data('Select All (This File)', f'extra {mode} all_this_file', 'header')
                buttons.button_data('Deselect All (This File)', f'extra {mode} deselect_all_this_file', 'header')
                buttons.button_data('Continue', f'extra {mode} continue', 'footer')
                buttons.button_data('Cancel', 'extra cancel', 'footer')
                selected_streams = self.executor.data['streams_to_remove']
                if selected_streams:
                    text += '\n<b>Selected for Removal (All Files):</b>\n'
                    for file in sorted_files:
                        file_selected = [key for key in selected_streams if key.startswith(f"{file}_")]
                        if file_selected:
                            text += f'<b>File: {file}</b>\n'
                            for key in file_selected:
                                text += f"- 🔴 {self.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')}\n"
            else:
                streams_dict = self.executor.data['streams']
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
                        f'Size: <b>{get_readable_file_size(self.executor.size)}</b>\n'
                        f'\n<b>Available Streams:</b>\n')
                if displayed_streams:
                    for i, (key, value) in enumerate(displayed_streams, start=start_idx + 1):
                        is_selected = key in self.executor.data['streams_to_remove']
                        LOGGER.info(f"Checking if {key} is selected: {is_selected}, streams_to_remove: {self.executor.data['streams_to_remove']}")
                        info = f"🔵 {value['info'].replace('🔵 ', '').replace('🔴 ', '')}" if is_selected else value['info'].replace('🔵 ', '').replace('🔴 ', '')
                        text += f"{i}. {info}\n"
                        buttons.button_data(f"{i}", f'extra {mode} {key}', 'footer')
                if total_streams > streams_per_page:
                    if page > 0:
                        buttons.button_data('Previous', f'extra {mode} prev', 'footer')
                    if page < total_pages - 1:
                        buttons.button_data('Next', f'extra {mode} next', 'footer')
                buttons.button_data('Select All', f'extra {mode} all', 'footer')
                buttons.button_data('Reset', f'extra {mode} reset', 'header')
                buttons.button_data('Reverse', f'extra {mode} reverse', 'header')
                if mode == 'rmstream':
                    buttons.button_data('All Audio', f'extra {mode} all audio', 'footer')
                    buttons.button_data('All Subtitles', f'extra {mode} all subtitle', 'footer')
                has_selections = bool(self.executor.data['streams_to_remove'])
                buttons.button_data('Continue' if not has_selections else 'Continue (Remove)', f'extra {mode} continue', 'footer')
                buttons.button_data('Cancel', 'extra cancel', 'footer')
                if has_selections and mode in ['merge_rmaudio', 'rmstream']:
                    text += '\n<b>Selected for Removal:</b>\n'
                    for i, key in enumerate(self.executor.data['streams_to_remove'], start=1):
                        if key in streams_dict:
                            text += f"{i}. 🔴 {streams_dict[key]['info'].replace('🔵 ', '').replace('🔴 ', '')}\n"

            elapsed_time = int(time() - self._time)
            remaining_time = max(0, 180 - elapsed_time)
            text += f'\n<i>Time Left: {get_readable_time(remaining_time)}</i>'
            LOGGER.info(f"Prepared streams_select text for {mode}")
            return text, buttons.build_menu(2)

    async def get_buttons(self, *args):
        """Initiates the stream selection process and awaits user input."""
        LOGGER.info(f"Starting get_buttons for mode {self.executor.mode} with args: {args}")
        future = self._event_handler()
        try:
            if not args or not args[0]:
                LOGGER.error(f"No valid streams passed to get_buttons for {self.executor.mode}")
                await self._cleanup()
                return
            if self.executor.mode == 'merge_preremove_audio':
                await self.update_message(*(await self.streams_select(self.executor.data.get('streams_per_file', {}), 'merge_preremove_audio')))
            else:
                await self.update_message(*(await self.streams_select(*args, self.executor.mode)))
            await wrap_future(future)
            LOGGER.info(f"get_buttons finished awaiting event for {self.executor.mode}, data: {self.executor.data}")
        except Exception as e:
            LOGGER.error(f"Error in get_buttons for {self.executor.mode}: {e}", exc_info=True)
            await self._cleanup()
        finally:
            if self._reply:
                await deleteMessage(self._reply)
                LOGGER.info(f"Deleted reply message for {self.executor.mode}")
            self.executor.event.set()
            if self.is_cancel:
                self._listener.suproc = 'cancelled'
                await self._listener.onUploadError(f'{VID_MODE[self.executor.mode]} stopped by user!')

async def cb_extra(_, query: CallbackQuery, obj: ExtraSelect):
    """Handles callback queries for stream selection actions."""
    data = query.data.split()
    if len(data) < 2:
        await query.answer("Invalid callback data!", show_alert=True)
        LOGGER.warning(f"Invalid callback data: {query.data}")
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
        elif mode == 'merge_preremove_audio':
            if data[2] == 'prev_file':
                obj.current_file_index = max(0, obj.current_file_index - 1)
            elif data[2] == 'next_file':
                obj.current_file_index = min(len(obj.executor.data['sorted_files']) - 1, obj.current_file_index + 1)
            elif data[2] == 'all_this_file':
                current_file = obj.executor.data['sorted_files'][obj.current_file_index]
                for key in obj.executor.data['streams']:
                    if key.startswith(f"{current_file}_") and key not in obj.executor.data['streams_to_remove']:
                        obj.executor.data['streams_to_remove'].append(key)
                        obj.executor.data['streams'][key]['info'] = f"🔵 {obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')}"
                        LOGGER.info(f"Selected all streams for {current_file}: {obj.executor.data['streams_to_remove']}")
            elif data[2] == 'deselect_all_this_file':
                current_file = obj.executor.data['sorted_files'][obj.current_file_index]
                obj.executor.data['streams_to_remove'] = [key for key in obj.executor.data['streams_to_remove'] if not key.startswith(f"{current_file}_")]
                for key in obj.executor.data['streams']:
                    if key.startswith(f"{current_file}_"):
                        obj.executor.data['streams'][key]['info'] = obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')
                LOGGER.info(f"Deselected all streams for {current_file}: {obj.executor.data['streams_to_remove']}")
            elif data[2] == 'continue':
                LOGGER.info(f"Continue triggered for {mode}, selections: {obj.executor.data.get('streams_to_remove', [])}")
                obj.event.set()
                LOGGER.info(f"Event set for {mode}, is_set: {obj.event.is_set()}")
            else:
                try:
                    stream_key = data[2]
                    if stream_key in obj.executor.data['streams']:
                        streams_to_remove = obj.executor.data['streams_to_remove']
                        if stream_key in streams_to_remove:
                            streams_to_remove.remove(stream_key)
                            obj.executor.data['streams'][stream_key]['info'] = obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '').replace('🔴 ', '')
                            LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                        else:
                            streams_to_remove.append(stream_key)
                            obj.executor.data['streams'][stream_key]['info'] = f"🔵 {obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '').replace('🔴 ', '')}"
                            LOGGER.info(f"Selected stream {stream_key} for {mode}, streams_to_remove: {streams_to_remove}")
                except (ValueError, KeyError) as e:
                    LOGGER.error(f"Invalid stream key or data error: {e}")
                    await obj.update_message(f"Error selecting stream {data[2]}. Please try again.", buttons.build_menu(2))
            await obj.update_message(*(await obj.streams_select(mode=mode)))
        elif data[2] in ['prev', 'next']:
            obj.stream_page[mode] = max(0, obj.stream_page.get(mode, 0) + (1 if data[2] == 'next' else -1))
            LOGGER.info(f"Page navigation: {data[2]} for {mode}, new page: {obj.stream_page[mode]}")
            await obj.update_message(*(await obj.streams_select(mode=mode)))
        elif data[2] == 'reset':
            LOGGER.info(f"Resetting selections for {mode}")
            for key in obj.executor.data.get('streams_to_remove', []):
                if key in obj.executor.data['streams']:
                    obj.executor.data['streams'][key]['info'] = obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')
            obj.executor.data['streams_to_remove'] = []
            obj.executor.data['sdata'] = []
            await obj.update_message(*(await obj.streams_select(mode=mode)))
        elif data[2] == 'continue':
            LOGGER.info(f"Continue triggered for {mode}, selections: {obj.executor.data.get('streams_to_remove', [])}")
            obj.event.set()
            LOGGER.info(f"Event set for {mode}, is_set: {obj.event.is_set()}")
        elif mode == 'merge_rmaudio':
            if data[2] == 'all':
                LOGGER.info(f"Selecting all streams to remove for {mode}")
                obj.executor.data['streams_to_remove'] = list(obj.executor.data['streams'].keys())
                for key in obj.executor.data['streams_to_remove']:
                    obj.executor.data['streams'][key]['info'] = f"🔵 {obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')}"
            elif data[2] == 'reverse':
                LOGGER.info(f"Reversing selections for {mode}")
                all_streams = list(obj.executor.data['streams'].keys())
                obj.executor.data['streams_to_remove'] = [s for s in all_streams if s not in obj.executor.data['streams_to_remove']]
                for key in all_streams:
                    obj.executor.data['streams'][key]['info'] = f"🔵 {obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')}" if key in obj.executor.data['streams_to_remove'] else obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')
            else:
                try:
                    stream_key = int(data[2])
                    streams_to_remove = obj.executor.data.get('streams_to_remove', [])
                    if stream_key in streams_to_remove:
                        streams_to_remove.remove(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '').replace('🔴 ', '')
                        LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                    else:
                        streams_to_remove.append(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = f"🔵 {obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '').replace('🔴 ', '')}"
                        LOGGER.info(f"Selected stream {stream_key} for {mode}, streams_to_remove: {streams_to_remove}")
                    obj.executor.data['streams_to_remove'] = streams_to_remove
                except (ValueError, KeyError) as e:
                    LOGGER.error(f"Invalid stream key or data error: {e}")
                    await obj.update_message(f"Error selecting stream {data[2]}. Please try again.", buttons.build_menu(2))
            await obj.update_message(*(await obj.streams_select(mode=mode)))
        elif mode == 'rmstream':
            if data[2] == 'all':
                if len(data) > 3 and 'audio' in data[3].lower():
                    LOGGER.info(f"Selecting all audio streams for {mode}")
                    obj.executor.data['sdata'] = [s['index'] for s in obj.executor.data['streams'].values() if s['codec_type'] == 'audio']
                elif len(data) > 3 and 'subtitle' in data[3].lower():
                    LOGGER.info(f"Selecting all subtitle streams for {mode}")
                    obj.executor.data['sdata'] = [s['index'] for s in obj.executor.data['streams'].values() if s['codec_type'] == 'subtitle']
                else:
                    LOGGER.info(f"Selecting all streams for {mode}")
                    obj.executor.data['sdata'] = list(obj.executor.data['streams'].keys())
                for key in obj.executor.data['sdata']:
                    obj.executor.data['streams'][key]['info'] = f"🔵 {obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')}"
            elif data[2] == 'reverse':
                LOGGER.info(f"Reversing selections for {mode}")
                all_streams = list(obj.executor.data['streams'].keys())
                obj.executor.data['sdata'] = [s for s in all_streams if s not in obj.executor.data.get('sdata', [])]
                for key in all_streams:
                    obj.executor.data['streams'][key]['info'] = f"🔵 {obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')}" if key in obj.executor.data['sdata'] else obj.executor.data['streams'][key]['info'].replace('🔵 ', '').replace('🔴 ', '')
            else:
                try:
                    stream_key = int(data[2])
                    sdata = obj.executor.data.get('sdata', [])
                    if stream_key in sdata:
                        sdata.remove(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '').replace('🔴 ', '')
                        LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                    else:
                        sdata.append(stream_key)
                        obj.executor.data['streams'][stream_key]['info'] = f"🔵 {obj.executor.data['streams'][stream_key]['info'].replace('🔵 ', '').replace('🔴 ', '')}"
                        LOGGER.info(f"Selected stream {stream_key} for {mode}")
                    obj.executor.data['sdata'] = sdata
                except (ValueError, KeyError) as e:
                    LOGGER.error(f"Invalid stream index or data error: {e}")
                    await obj.update_message(f"Error selecting stream {data[2]}. Please try again.", buttons.build_menu(2))
            await obj.update_message(*(await obj.streams_select(mode=mode)))
        elif mode == 'extract':
            if data[2] == 'alt':
                obj.executor.data['alt_mode'] = not literal_eval(data[3])
                LOGGER.info(f"Toggled ALT mode to {obj.executor.data['alt_mode']} for {mode}")
                await obj.update_message(*(await obj.streams_select(mode=mode)))
            elif data[2] == 'extension':
                ext_idx = {'video': 2, 'audio': 0, 'subtitle': 1}
                current_ext = obj.extension[ext_idx.get(data[3], 2)]
                obj.extension[ext_idx.get(data[3], 2)] = None if current_ext else data[3] if data[3] != 'default' else None
                LOGGER.info(f"Updated extension for {data[3]} to {obj.extension[ext_idx.get(data[3], 2)]}")
                await obj.update_message(*(await obj.streams_select(mode=mode)))
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
            else:
                LOGGER.info(f"Cancel triggered in compress mode")
                obj.is_cancel = obj.executor.is_cancel = True
                await obj._cleanup()
                obj.event.set()
        elif mode == 'convert':
            if data[2] != 'cancel':
                obj.executor.data = data[2]
                LOGGER.info(f"Convert resolution selected: {data[2]}")
                obj.event.set()
            else:
                LOGGER.info(f"Cancel triggered in convert mode")
                obj.is_cancel = obj.executor.is_cancel = True
                await obj._cleanup()
                obj.event.set()
        elif mode == 'subsync':
            if data[2] == 'select':
                try:
                    obj.executor.data['final'][obj.status] = {
                        'file': obj.executor.data['list'][obj.status],
                        'ref': obj.executor.data['list'][int(data[3])]
                    }
                    LOGGER.info(f"Subsync reference selected: {obj.executor.data['list'][int(data[3])]} for {obj.executor.data['list'][obj.status]}")
                    obj.status = ''
                    await obj.update_message(*(await obj.streams_select(mode=mode)))
                except (KeyError, ValueError) as e:
                    LOGGER.error(f"Error in subsync select: {e}")
            elif not obj.status and data[2].isdigit():
                obj.status = int(data[2])
                LOGGER.info(f"Subsync status set to: {obj.status}")
                await obj.update_message(*(await obj.streams_select(mode=mode)))
            elif obj.status and data[2] == 'continue':
                LOGGER.info(f"Subsync continue triggered")
                obj.event.set()