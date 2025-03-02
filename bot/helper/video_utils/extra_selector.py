from __future__ import annotations
from functools import partial
from pyrogram.filters import regex, user
from pyrogram.handlers import CallbackQueryHandler
from pyrogram.types import CallbackQuery
from time import time
import asyncio

from bot import LOGGER, VID_MODE
from bot.helper.ext_utils.bot_utils import new_thread
from bot.helper.ext_utils.status_utils import get_readable_file_size, get_readable_time
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, deleteMessage
from bot.helper.video_utils import executor as exc

class ExtraSelect:
    def __init__(self, executor: exc.VidEcxecutor):
        self._listener = executor.listener
        self._time = time()
        self._reply = None
        self.executor = executor
        self.is_cancel = False
        self.current_file_index = 0
        self._done = False

    @new_thread
    async def _event_handler(self):
        LOGGER.info(f"Starting ExtraSelect event handler for {self.executor.mode} (MID: {self.executor.listener.mid})")
        pfunc = partial(cb_extra, obj=self)
        handler = None
        try:
            handler = self._listener.client.add_handler(
                CallbackQueryHandler(pfunc, filters=regex('^extra') & user(self._listener.user_id)), group=-1)
            while not self._done and (time() - self._time) < 180:
                await asyncio.sleep(1)
            if not self._done:
                LOGGER.warning(f"ExtraSelect timed out for {self.executor.mode}")
                self.is_cancel = True
                await self._cleanup()
            else:
                LOGGER.info(f"ExtraSelect completed successfully for {self.executor.mode}")
        except Exception as e:
            LOGGER.error(f"Event handler error in ExtraSelect: {str(e)}", exc_info=True)
            self.is_cancel = True
            await self._cleanup()
        finally:
            if handler:
                self._listener.client.remove_handler(*handler)

    async def _cleanup(self):
        LOGGER.info(f"Cleaning up ExtraSelect for {self.executor.mode}")
        if self.is_cancel:
            self.executor.data.clear()
        self.executor.is_cancel = True
        if self._reply:
            await deleteMessage(self._reply)
            self._reply = None

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
            LOGGER.error(f"Failed to update message: {e}", exc_info=True)

    def _format_stream_name(self, stream):
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    async def streams_select(self, streams=None, mode=None):
        buttons = ButtonMaker()
        if not self.executor.data:
            if not streams:
                LOGGER.warning(f"No streams provided for {mode}")
                return "No streams available.", buttons.build_menu(1)
            LOGGER.info(f"Initializing stream data for {mode}")
            self.executor.data = {'streams': {}, 'streams_to_remove': []}
            if isinstance(streams, list):  # merge_rmaudio
                for stream in streams:
                    if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                        index = stream['index']
                        self.executor.data['streams'][index] = stream
                        self.executor.data['streams'][index]['info'] = self._format_stream_name(stream)
            elif isinstance(streams, dict):  # merge_preremove_audio
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
            current_streams = [s for k, s in self.executor.data['streams'].items() if k.startswith(f"{current_file}_")]
            text = (f'<b>{VID_MODE[mode].upper()} ~ {self._listener.tag}</b>\n'
                    f'<code>{current_file}</code>\n'
                    f'Size: <b>{get_readable_file_size(self.executor.size)}</b>\n'
                    f'\n<b>Streams for {current_file}:</b>\n')
            for i, stream in enumerate(current_streams, start=1):
                key = f"{current_file}_{stream['index']}"
                is_selected = key in self.executor.data['streams_to_remove']
                info = f"🔵 {stream['info']}" if is_selected else stream['info']
                text += f"{i}. {info}\n"
                buttons.button_data(f"{i}", f'extra {mode} {key}', 'footer')
            if self.current_file_index > 0:
                buttons.button_data('Previous File', f'extra {mode} prev_file', 'footer')
            if self.current_file_index < len(sorted_files) - 1:
                buttons.button_data('Next File', f'extra {mode} next_file', 'footer')
            buttons.button_data('Select All', f'extra {mode} all', 'header')
            buttons.button_data('Deselect All', f'extra {mode} reset', 'header')
        else:  # merge_rmaudio
            streams_dict = self.executor.data['streams']
            text = (f'<b>{VID_MODE[mode].upper()} ~ {self._listener.tag}</b>\n'
                    f'<code>{self.executor.name}</code>\n'
                    f'Size: <b>{get_readable_file_size(self.executor.size)}</b>\n'
                    f'\n<b>Available Streams:</b>\n')
            for i, (key, value) in enumerate(streams_dict.items(), start=1):
                is_selected = key in self.executor.data['streams_to_remove']
                info = f"🔵 {value['info']}" if is_selected else value['info']
                text += f"{i}. {info}\n"
                buttons.button_data(f"{i}", f'extra {mode} {key}', 'footer')
            buttons.button_data('Select All', f'extra {mode} all', 'header')
            buttons.button_data('Deselect All', f'extra {mode} reset', 'header')

        buttons.button_data('Continue', f'extra {mode} continue', 'footer')
        buttons.button_data('Cancel', 'extra cancel', 'footer')
        if self.executor.data['streams_to_remove']:
            text += '\n<b>Selected for Removal:</b>\n'
            for i, key in enumerate(self.executor.data['streams_to_remove'], start=1):
                text += f"{i}. {self.executor.data['streams'][key]['info']}\n"

        elapsed_time = int(time() - self._time)
        remaining_time = max(0, 180 - elapsed_time)
        text += f'\n<i>Time Left: {get_readable_time(remaining_time)}</i>'
        LOGGER.info(f"Prepared streams_select text for {mode}")
        return text, buttons.build_menu(2)

    async def get_buttons(self, *args):
        LOGGER.info(f"Starting get_buttons for mode {self.executor.mode} with args: {args}")
        if not args or not args[0]:
            LOGGER.error(f"No valid streams passed to get_buttons for {self.executor.mode}")
            await self._cleanup()
            return
        await self.update_message(*(await self.streams_select(*args, self.executor.mode)))
        await self._event_handler()
        if self.is_cancel:
            self._listener.suproc = 'cancelled'
            await self._listener.onUploadError(f'{VID_MODE[self.executor.mode]} stopped by user!')
        else:
            LOGGER.info(f"Selections completed, data: {self.executor.data}")
            await self.executor.on_selection_complete()
        if self._reply:
            await deleteMessage(self._reply)
            LOGGER.info(f"Deleted reply message for {self.executor.mode}")

async def cb_extra(_, query: CallbackQuery, obj: ExtraSelect):
    data = query.data.split()
    if len(data) < 2:
        await query.answer("Invalid callback data!", show_alert=True)
        LOGGER.warning(f"Invalid callback data: {query.data}")
        return
    mode = data[1]
    await query.answer()
    LOGGER.info(f"Received callback: {query.data}")

    if data[2] == 'cancel':
        LOGGER.info(f"Cancel triggered for {mode}")
        obj.is_cancel = obj.executor.is_cancel = True
        await obj._cleanup()
        obj._done = True
    elif mode == 'merge_preremove_audio':
        if data[2] == 'prev_file':
            obj.current_file_index = max(0, obj.current_file_index - 1)
        elif data[2] == 'next_file':
            obj.current_file_index = min(len(obj.executor.data['sorted_files']) - 1, obj.current_file_index + 1)
        elif data[2] == 'all':
            current_file = obj.executor.data['sorted_files'][obj.current_file_index]
            for key in obj.executor.data['streams']:
                if key.startswith(f"{current_file}_") and key not in obj.executor.data['streams_to_remove']:
                    obj.executor.data['streams_to_remove'].append(key)
            LOGGER.info(f"Selected all streams for {current_file}: {obj.executor.data['streams_to_remove']}")
        elif data[2] == 'reset':
            current_file = obj.executor.data['sorted_files'][obj.current_file_index]
            obj.executor.data['streams_to_remove'] = [k for k in obj.executor.data['streams_to_remove'] if not k.startswith(f"{current_file}_")]
            LOGGER.info(f"Deselected all streams for {current_file}: {obj.executor.data['streams_to_remove']}")
        elif data[2] == 'continue':
            LOGGER.info(f"Continue triggered for {mode}, selections: {obj.executor.data.get('streams_to_remove', [])}")
            obj._done = True
            return
        else:
            try:
                stream_key = data[2]
                if stream_key in obj.executor.data['streams']:
                    streams_to_remove = obj.executor.data['streams_to_remove']
                    if stream_key in streams_to_remove:
                        streams_to_remove.remove(stream_key)
                        LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                    else:
                        streams_to_remove.append(stream_key)
                        LOGGER.info(f"Selected stream {stream_key} for {mode}, streams_to_remove: {streams_to_remove}")
            except (ValueError, KeyError) as e:
                LOGGER.error(f"Invalid stream key or data error: {e}")
                await obj.update_message(f"Error selecting stream {data[2]}.", buttons.build_menu(2))
        await obj.update_message(*(await obj.streams_select(mode=mode)))
    elif mode == 'merge_rmaudio':
        if data[2] == 'all':
            LOGGER.info(f"Selecting all streams to remove for {mode}")
            obj.executor.data['streams_to_remove'] = [int(k) for k in obj.executor.data['streams'].keys()]
        elif data[2] == 'reset':
            LOGGER.info(f"Resetting selections for {mode}")
            obj.executor.data['streams_to_remove'] = []
        elif data[2] == 'continue':
            LOGGER.info(f"Continue triggered for {mode}, selections: {obj.executor.data.get('streams_to_remove', [])}")
            obj._done = True
            return
        else:
            try:
                stream_key = int(data[2])
                if stream_key in obj.executor.data['streams']:
                    streams_to_remove = obj.executor.data['streams_to_remove']
                    if stream_key in streams_to_remove:
                        streams_to_remove.remove(stream_key)
                        LOGGER.info(f"Deselected stream {stream_key} for {mode}")
                    else:
                        streams_to_remove.append(stream_key)
                        LOGGER.info(f"Selected stream {stream_key} for {mode}, streams_to_remove: {streams_to_remove}")
            except (ValueError, KeyError) as e:
                LOGGER.error(f"Invalid stream key or data error: {e}")
                await obj.update_message(f"Error selecting stream {data[2]}.", buttons.build_menu(2))
        await obj.update_message(*(await obj.streams_select(mode=mode)))