from __future__ import annotations
from ast import literal_eval
from asyncio import Event, wait_for, wrap_future, gather
from functools import partial
from pyrogram.filters import regex, user
from pyrogram.handlers import CallbackQueryHandler
from pyrogram.types import CallbackQuery
from time import time
import os
from bot import LOGGER  # Added for debugging

from bot import VID_MODE
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
        self.event = Event()
        self.is_cancel = False
        self.extension: list[str] = [None, None, 'mkv']
        self.status = ''
        self.stream_page = {}  # Track pagination per mode

    @new_thread
    async def _event_handler(self):
        pfunc = partial(cb_extra, obj=self)
        handler = self._listener.client.add_handler(CallbackQueryHandler(pfunc, filters=regex('^extra') & user(self._listener.user_id)), group=-1)
        try:
            await wait_for(self.event.wait(), timeout=180)
        except:
            self.event.set()
        finally:
            self._listener.client.remove_handler(*handler)

    async def update_message(self, text: str, buttons):
        if not self._reply:
            self._reply = await sendMessage(text, self._listener.message, buttons)
        else:
            await editMessage(text, self._reply, buttons)

    def _format_stream_name(self, stream):
        """Format stream name with codec, type, and language."""
        codec_type = stream.get('codec_type', 'unknown').title()
        codec_name = stream.get('codec_name', 'Unknown')
        lang = stream.get('tags', {}).get('language', 'Unknown').upper()
        resolution = f" ({stream.get('height', '')}p)" if stream.get('codec_type') == 'video' and stream.get('height', '') else ''
        return f"{codec_type} ~ {codec_name} ({lang}){resolution}"

    def streams_select(self, streams, mode=None):
        buttons = ButtonMaker()
        if not self.executor.data:
            self.executor.data.setdefault('streams', {})
            if mode in ['merge_rmaudio', 'merge_preremove_audio', 'rmstream']:
                self.executor.data['streams_to_remove'] = self.executor.data.get('streams_to_remove', [])  # Track all streams for removal
            elif mode == 'extract':
                self.executor.data['streams_to_extract'] = self.executor.data.get('streams_to_extract', [])  # Track streams for extraction
            self.executor.data['sdata'] = []  # For rmstream mode

            if isinstance(streams, list):  # For single file modes (e.g., merge_rmaudio, rmstream)
                for stream in streams:
                    if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                        index = stream['index']
                        self.executor.data['streams'][index] = stream
                        stream_info = self._format_stream_name(stream)
                        if mode == 'rmstream':
                            selected = index in self.executor.data['sdata']
                            self.executor.data['streams'][index]['info'] = f"{'🔵 ' if selected else ''}{stream_info}"
                        else:
                            self.executor.data['streams'][index]['info'] = stream_info
            elif isinstance(streams, dict):  # For merge_preremove_audio with per-file streams
                for file, file_streams in streams.items():
                    for stream in file_streams:
                        if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                            index = stream['index']
                            self.executor.data['streams'][f"{file}_{index}"] = stream
                            stream_info = self._format_stream_name(stream)
                            self.executor.data['streams'][f"{file}_{index}"]['info'] = stream_info
            elif isinstance(streams, tuple) and len(streams) == 2:  # Handle tuple from get_metavideo
                stream_list, _ = streams  # Unpack the tuple, use only streams list
                for stream in stream_list:
                    if isinstance(stream, dict) and stream.get('codec_type') in ['video', 'audio', 'subtitle']:
                        index = stream['index']
                        self.executor.data['streams'][index] = stream
                        stream_info = self._format_stream_name(stream)
                        if mode == 'rmstream':
                            selected = index in self.executor.data['sdata']
                            self.executor.data['streams'][index]['info'] = f"{'🔵 ' if selected else ''}{stream_info}"
                        else:
                            self.executor.data['streams'][index]['info'] = stream_info

        mode, ddict = self.executor.mode, self.executor.data
        streams_dict = ddict['streams']
        self.stream_page.setdefault(mode, 0)  # Initialize pagination per mode
        streams_per_page = 5
        total_streams = len(streams_dict)
        total_pages = max(1, (total_streams + streams_per_page - 1) // streams_per_page)
        start_idx = self.stream_page[mode] * streams_per_page
        end_idx = min((self.stream_page[mode] + 1) * streams_per_page, total_streams)
        displayed_streams = list(streams_dict.items())[start_idx:end_idx]

        text = (f'<b>{VID_MODE[mode].upper()} ~ {self._listener.tag}</b>\n'
                f'<code>{self.executor.name}</code>\n'
                f'File Size: <b>{get_readable_file_size(self.executor.size)}</b>\n')
        
        if displayed_streams:
            text += '\nAvailable Streams:\n'
            for key, value in displayed_streams:
                if mode == 'rmstream':
                    buttons.button_data(value['info'], f'extra {mode} {key}')
                else:
                    buttons.button_data(value['info'], f'extra {mode} {key}')
        else:
            text += '\nNo streams available for selection.'

        if mode == 'extract':
            buttons.button_data('🔥 ALT Mode' if ddict.get('alt_mode') else 'ALT Mode', f"extra {mode} alt {ddict.get('alt_mode', False)}", 'footer')
            audext, subext, vidext = self.extension
            text += (f"\n<b>┌ </b>Video Format: <b>{vidext.upper()}</b>\n"
                     f'<b>├ </b>Audio Format: <b>{audext.upper()}</b>\n'
                     f'<b>└ </b>Subtitle Format: <b>{subext.upper()}</b>')
            for ext in self.extension:
                buttons.button_data(ext.upper(), f'extra {mode} extension {ext}', 'header')
            buttons.button_data('Extract All', f'extra {mode} video audio subtitle')

        if mode in ['merge_rmaudio', 'merge_preremove_audio', 'rmstream']:
            if mode == 'rmstream':
                if ddict.get('sdata'):
                    text += '\n\nStreams to remove:\n'
                    for i, sindex in enumerate(ddict['sdata'], start=1):
                        text += f"{i}. {ddict['streams'][sindex]['info'].replace('🔵 ', '')}\n"
                buttons.button_data('All Audio', f'extra {mode} audio')
                buttons.button_data('All Subs', f'extra {mode} subtitle')
            else:
                if ddict.get('streams_to_remove'):
                    text += '\n\nStreams to remove:\n'
                    for stream_key in ddict['streams_to_remove']:
                        stream = ddict['streams'][stream_key]
                        text += f"- {stream['info']}\n"
            buttons.button_data('Remove All', f'extra {mode} all')
            buttons.button_data('Reset', f'extra {mode} reset', 'header')
            buttons.button_data('Reverse', f'extra {mode} reverse', 'header')
            if mode == 'rmstream':
                buttons.button_data('Continue', f'extra {mode} continue', 'footer')
            else:
                continue_label = "Continue (Remove Selected)" if ddict.get('streams_to_remove') else "Continue (Keep All)"
                buttons.button_data(continue_label, f'extra {mode} continue', 'footer')

        buttons.button_data('Cancel', 'extra cancel', 'footer')
        if total_streams > streams_per_page:
            if self.stream_page[mode] > 0:
                buttons.button_data('Previous', f'extra {mode} prev', 'footer')
            if self.stream_page[mode] < total_pages - 1:
                buttons.button_data('Next', f'extra {mode} next', 'footer')

        text += f'\n\n<i>Time Out: {get_readable_time(180 - (time()-self._time))}</i>'
        return text, buttons.build_menu(2)

    async def merge_rmaudio_select(self, streams):
        if isinstance(streams, tuple) and len(streams) == 2:  # Handle tuple from get_metavideo
            streams, _ = streams  # Unpack the tuple, use only streams list
        await self.streams_select(streams, 'merge_rmaudio')

    async def merge_preremove_audio_select(self, streams_per_file: dict):
        await self.streams_select(streams_per_file, 'merge_preremove_audio')

    async def compress_select(self, streams: dict):
        if isinstance(streams, tuple) and len(streams) == 2:  # Handle tuple from get_metavideo
            streams, _ = streams  # Unpack the tuple, use only streams list
        self.executor.data = {}
        buttons = ButtonMaker()
        for stream in streams:
            indexmap, codec_type, lang = stream.get('index'), stream.get('codec_type'), stream.get('tags', {}).get('language')
            if not lang:
                lang = str(indexmap)
            if codec_type == 'video' and indexmap == 0:
                self.executor.data['video'] = indexmap
            if codec_type == 'video' and 'video' not in self.executor.data:
                self.executor.data['video'] = indexmap
            if codec_type == 'audio':
                buttons.button_data(f'Audio ~ {lang.upper()}', f'extra compress {indexmap}')
        buttons.button_data('Continue', 'extra compress 0')
        buttons.button_data('Cancel', 'extra cancel')
        await self.update_message(f'{self._listener.tag}, Select available audio or press <b>Continue (no audio)</b>.\n<code>{self.executor.name}</code>', buttons.build_menu(2))

    async def rmstream_select(self, streams):
        if isinstance(streams, tuple) and len(streams) == 2:  # Handle tuple from get_metavideo
            streams, _ = streams  # Unpack the tuple, use only streams list
        await self.streams_select(streams, 'rmstream')

    async def convert_select(self, streams: dict):
        if isinstance(streams, tuple) and len(streams) == 2:  # Handle tuple from get_metavideo
            streams, _ = streams  # Unpack the tuple, use only streams list
        buttons = ButtonMaker()
        hvid = '1080p'
        resolution = {'1080p': 'Convert 1080p',
                      '720p': 'Convert 720p',
                      '540p': 'Convert 540p',
                      '480p': 'Convert 480p',
                      '360p': 'Convert 360p'}
        for stream in streams:
            if stream['codec_type'] == 'video':
                vid_height = f'{stream["height"]}p'
                if vid_height in resolution:
                    hvid = vid_height
                break
        keys = list(resolution)
        for key in keys[keys.index(hvid)+1:]:
            buttons.button_data(resolution[key], f'extra convert {key}')
        buttons.button_data('Cancel', 'extra cancel', 'footer')
        await self.update_message(f'{self._listener.tag}, Select available resolution to convert.\n<code>{self.executor.name}</code>', buttons.build_menu(2))

    async def subsync_select(self):
        buttons = ButtonMaker()
        text = ''
        index = 1
        if not self.status:
            self.executor.data['list'] = {}
            for position, file in enumerate(natsorted(await listdir(self.executor.path)), 1):
                file_path = os.path.join(self.executor.path, file)
                if (await exc.get_document_type(file_path))[0] or file.endswith(('.srt', '.ass')):
                    self.executor.data['list'][position] = file
                    index += 1
            if not self.executor.data['list']:
                return self.executor._up_path
            self.executor.data['final'] = {}
            self._start_handler()
            await gather(self._send_status(), self.event.wait())

            if self.is_cancel:
                return
            if not self.data or not self.data.get('final'):
                return self.executor._up_path
            sub_files = [os.path.join(self.executor.path, key['file']) for key in self.data['final'].values()]
            ref_files = [os.path.join(self.executor.path, key['ref']) for key in self.data['final'].values()]
        else:
            file = self.executor.data['list'][self.status]
            text = (f'Current: <b>{file}</b>\n'
                    f'References: <b>{ref}</b>\n' if (ref := self.executor.data['final'].get(self.status, {}).get('ref')) else ''
                    '\nSelect Available References Below!\n')
            self.executor.data['final'][self.status] = {'file': file}
            for position, file in self.executor.data['list'].items():
                if position != self.status and file not in [v['file'] for v in self.executor.data['final'].values()]:
                    text += f'{index}. {file}\n'
                    buttons.button_data(str(index), f'extra subsync select {position}')
                    index += 1
        await self.update_message(text, buttons.build_menu(5))

    async def extract_select(self, streams: dict):
        if isinstance(streams, tuple) and len(streams) == 2:  # Handle tuple from get_metavideo
            streams, _ = streams  # Unpack the tuple, use only streams list
        self.executor.data = {}
        ext = [None, None, 'mkv']
        for stream in streams:
            codec_name, codec_type = stream.get('codec_name'), stream.get('codec_type')
            if codec_type == 'audio' and not ext[0]:
                match codec_type:
                    case 'mp3':
                        ext[0] = 'ac3'
                    case 'aac' | 'ac3' | 'eac3' | 'm4a' | 'mka' | 'wav' as value:
                        ext[0] = value
                    case _:
                        ext[0] = 'aac'
            elif codec_type == 'subtitle' and not ext[1]:
                ext[1] = 'srt' if codec_name == 'subrip' else 'ass'
        if not ext[0]:
            ext[0] = 'aac'
        if not ext[1]:
            ext[1] = 'srt'
        self.extension = ext
        await self.streams_select(streams, 'extract')

    async def get_buttons(self, *args):
        future = self._event_handler()
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
        self.executor.event.set()
        await deleteMessage(self._reply)
        if self.is_cancel:
            self._listener.suproc = 'cancelled'
            await self._listener.onUploadError(f'{VID_MODE[self.executor.mode]} stopped by user!')

async def cb_extra(_, query: CallbackQuery, obj: ExtraSelect):
    data = query.data.split()
    mode = data[1]
    
    await query.answer()
    if data[2] == 'cancel':
        obj.is_cancel = obj.executor.is_cancel = True
        obj.executor.data = None
        obj.event.set()
    elif data[2] in ['prev', 'next']:
        obj.stream_page[mode] = max(0, obj.stream_page.get(mode, 0) + (1 if data[2] == 'next' else -1))
        if mode in ['merge_rmaudio', 'merge_preremove_audio', 'rmstream', 'extract']:
            if mode == 'merge_rmaudio':
                await obj.merge_rmaudio_select(None)
            elif mode == 'merge_preremove_audio':
                await obj.merge_preremove_audio_select(obj.executor.data['streams_per_file'])
            elif mode == 'rmstream':
                await obj.rmstream_select(obj.executor.data['streams'])
            elif mode == 'extract':
                await obj.extract_select(obj.executor.data['streams'])
    elif mode == 'merge_rmaudio':
        if data[2] == 'all':
            obj.executor.data['streams_to_remove'] = list(obj.executor.data['streams'].keys())
            obj.event.set()
        elif data[2] == 'continue':
            obj.event.set()
        elif data[2] == 'reset':
            obj.executor.data['streams_to_remove'] = []
            await obj.merge_rmaudio_select(None)
        elif data[2] == 'reverse':
            all_streams = list(obj.executor.data['streams'].keys())
            obj.executor.data['streams_to_remove'] = [s for s in all_streams if s not in obj.executor.data['streams_to_remove']]
            await obj.merge_rmaudio_select(None)
        else:
            stream_key = int(data[2])
            streams_to_remove = obj.executor.data.get('streams_to_remove', [])
            if stream_key in streams_to_remove:
                streams_to_remove.remove(stream_key)
            else:
                streams_to_remove.append(stream_key)
            obj.executor.data['streams_to_remove'] = streams_to_remove
            LOGGER.info(f"merge_rmaudio: Updated streams_to_remove to {streams_to_remove}")
            await obj.merge_rmaudio_select(None)
    elif mode == 'merge_preremove_audio':
        files = list(obj.executor.data['streams_per_file'].keys())
        current_file = obj.status or files[0] if files else None
        
        if data[2] == 'done':
            obj.event.set()
        elif data[2] in ['prev_file', 'next_file']:
            obj.executor.data['file_page'] = max(0, obj.executor.data.get('file_page', 0) + (1 if data[2] == 'next_file' else -1))
            obj.status = files[obj.executor.data['file_page']]
            await obj.merge_preremove_audio_select(obj.executor.data['streams_per_file'])
        elif data[2] in ['prev_track', 'next_track']:
            total_pages = (len([s for s in obj.executor.data['streams_per_file'][current_file] 
                               if s['codec_type'] in ['video', 'audio', 'subtitle']]) + 4) // 5
            obj.executor.data['track_page'] = max(0, min(total_pages - 1, obj.executor.data.get('track_page', 0) + (1 if data[2] == 'next_track' else -1)))
            await obj.merge_preremove_audio_select(obj.executor.data['streams_per_file'])
        else:
            file, action = data[2], data[3]
            if file not in obj.executor.data['audio_selections']:
                obj.executor.data['audio_selections'][file] = []
            selections = obj.executor.data['audio_selections'].get(file, [])
            if action == 'all':
                obj.executor.data['audio_selections'][file] = [s['index'] for s in obj.executor.data['streams_per_file'][file] 
                                                            if s['codec_type'] in ['video', 'audio', 'subtitle']]
            elif action.isdigit():
                index = int(action)
                if index in selections:
                    selections.remove(index)
                else:
                    selections.append(index)
            obj.executor.data['audio_selections'][file] = selections
            obj.status = file
            await obj.merge_preremove_audio_select(obj.executor.data['streams_per_file'])
    elif mode == 'subsync':
        if data[2].isdigit():
            obj.status = int(data[2])
        elif data[2] == 'select':
            obj.executor.data['final'][obj.status]['ref'] = obj.executor.data['list'][int(data[3])]
            obj.status = ''
        elif data[2] == 'continue':
            obj.event.set()
            return
        await gather(query.answer(), obj.subsync_select())
    elif mode == 'compress':
        obj.executor.data['audio'] = int(data[2])
        obj.event.set()
    elif mode == 'convert':
        obj.executor.data = data[2]
        obj.event.set()
    elif mode == 'rmstream':
        if data[2] == 'all':
            if 'audio' in data[3].lower():
                obj.executor.data['sdata'] = [s['index'] for s in obj.executor.data['streams'].values() if s['codec_type'] == 'audio']
            elif 'subtitle' in data[3].lower():
                obj.executor.data['sdata'] = [s['index'] for s in obj.executor.data['streams'].values() if s['codec_type'] == 'subtitle']
            for index in obj.executor.data['sdata']:
                obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info']}"
        elif data[2] == 'continue':
            obj.event.set()
        elif data[2] == 'reset':
            for index in obj.executor.data.get('sdata', []):
                obj.executor.data['streams'][index]['info'] = obj.executor.data['streams'][index]['info'].replace('🔵 ', '')
            obj.executor.data['sdata'] = []
            await obj.rmstream_select(obj.executor.data['streams'])
        elif data[2] == 'reverse':
            all_streams = [s['index'] for s in obj.executor.data['streams'].values()]
            obj.executor.data['sdata'] = [s for s in all_streams if s not in obj.executor.data.get('sdata', [])]
            for index in all_streams:
                obj.executor.data['streams'][index]['info'] = f"🔵 {obj.executor.data['streams'][index]['info']}" if index in obj.executor.data['sdata'] else obj.executor.data['streams'][index]['info'].replace('🔵 ', '')
            await obj.rmstream_select(obj.executor.data['streams'])
        else:
            stream_index = int(data[2])
            if stream_index in obj.executor.data.get('sdata', []):
                obj.executor.data['sdata'].remove(stream_index)
                obj.executor.data['streams'][stream_index]['info'] = obj.executor.data['streams'][stream_index]['info'].replace('🔵 ', '')
            else:
                obj.executor.data['sdata'].append(stream_index)
                obj.executor.data['streams'][stream_index]['info'] = f"🔵 {obj.executor.data['streams'][stream_index]['info']}"
            await obj.rmstream_select(obj.executor.data['streams'])
    elif mode == 'extract':
        value = data[2]
        if value in ('extension', 'alt'):
            ext_dict = {'ass': [1, 'srt'],
                        'srt': [1, 'ass'],
                        'aac': [0, 'ac3'],
                        'ac3': [0, 'eac3'],
                        'eac3': [0, 'm4a'],
                        'm4a': [0, 'mka'],
                        'mka': [0, 'wav'],
                        'wav': [0, 'aac'],
                        'mp4': [2, 'mkv'],
                        'mkv': [2, 'mp4']}
            if data[3] in ext_dict:
                index, ext = ext_dict[data[3]]
                obj.extension[index] = ext
            if value == 'alt':
                obj.executor.data['alt_mode'] = not literal_eval(data[3])
            await obj.extract_select(obj.executor.data['streams'])
        else:
            obj.executor.data.update({'key': int(value) if value.isdigit() else data[2:],
                                     'extension': obj.extension})
            obj.event.set()