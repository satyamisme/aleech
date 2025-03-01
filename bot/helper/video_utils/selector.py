from __future__ import annotations
from aiofiles.os import path as aiopath, makedirs
from ast import literal_eval
from asyncio import Event, wait_for, wrap_future, gather
from functools import partial
from os import path as ospath
from PIL import Image
from pyrogram.filters import regex, user, text, photo, document
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import Message, CallbackQuery
from re import match as re_match
from time import time

from bot import config_dict, VID_MODE, LOGGER
from bot.helper.ext_utils.bot_utils import new_task, new_thread, sync_to_async
from bot.helper.ext_utils.files_utils import clean_target
from bot.helper.ext_utils.links_utils import is_media
from bot.helper.ext_utils.status_utils import get_readable_time
from bot.helper.listeners import tasks_listener as task
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, deleteMessage

class SelectMode:
    def __init__(self, listener: task.TaskListener, isLink=False):
        self._isLink = isLink
        self._time = time()
        self._reply = None
        self.listener = listener
        self.is_rename = False
        self.mode = ''
        self.extra_data = {}
        self.newname = ''
        self.event = Event()
        self.message_event = Event()
        self.is_cancelled = False

    @new_thread
    async def _event_handler(self):
        LOGGER.info(f"Starting SelectMode event handler for user {self.listener.user_id}")
        pfunc = partial(cb_vidtools, obj=self)
        handler = self.listener.client.add_handler(CallbackQueryHandler(pfunc, filters=regex('^vidtool') & user(self._listener.user_id)), group=-1)
        try:
            await wait_for(self.event.wait(), timeout=180)
            LOGGER.info(f"SelectMode event completed for user {self.listener.user_id}")
        except TimeoutError:
            self.mode = 'Task has been cancelled, time out!'
            self.is_cancelled = True
            self.event.set()
            LOGGER.warning(f"SelectMode timed out for user {self.listener.user_id}")
        except Exception as e:
            LOGGER.error(f"Event handler error in SelectMode: {str(e)}", exc_info=True)
            self.is_cancelled = True
            self.event.set()
        finally:
            self.listener.client.remove_handler(*handler)

    @new_thread
    async def message_event_handler(self, mode=''):
        LOGGER.info(f"Starting message event handler for mode {mode}")
        pfunc = partial(message_handler, obj=self, is_sub=mode == 'subfile')
        handler = self.listener.client.add_handler(MessageHandler(pfunc, user(self._listener.user_id)), group=1)
        try:
            await wait_for(self.message_event.wait(), timeout=60)
            LOGGER.info(f"Message event completed for mode {mode}")
        except TimeoutError:
            self.message_event.set()
            LOGGER.warning(f"Message event timed out for mode {mode}")
        except Exception as e:
            LOGGER.error(f"Message event handler error: {e}", exc_info=True)
            self.message_event.set()
        finally:
            self.listener.client.remove_handler(*handler)
            self.message_event.clear()

    async def _send_message(self, text: str, buttons):
        try:
            if not self._reply:
                self._reply = await sendMessage(text, self._listener.message, buttons)
                LOGGER.info(f"Sent initial message for mode selection")
            else:
                await editMessage(text, self._reply, buttons)
                LOGGER.info(f"Updated message for mode selection")
        except Exception as e:
            LOGGER.error(f"Failed to send message: {e}")

    def _captions(self, mode: str=None):
        msg = (f'<b>VIDEOS TOOL SETTINGS</b>\n'
               f'Mode: <b>{VID_MODE.get(self.mode, "None")}</b>\n'
               f'Name: <b>{self.newname or "Default"}</b>')
        if self.extra_data and self.mode == 'trim':
            msg += f'\nTrim Duration: <b>{list(self.extra_data.values())}</b>'
        if self.mode in ('vid_sub', 'watermark'):
            hardsub = self.extra_data.get('hardsub')
            msg += f"\nHardsub Mode: <b>{'Enable' if hardsub else 'Disable'}</b>"
            if hardsub:
                msg += f"\nBold Style: <b>{'Enable' if self.extra_data.get('boldstyle') else 'Disable'}</b>"
                fontname = self.extra_data.get('fontname') or config_dict['HARDSUB_FONT_NAME']
                msg += f'\nFont Name: <b>{fontname.replace("_", " ")}</b>'
                fontsize = self.extra_data.get('fontsize') or config_dict['HARDSUB_FONT_SIZE']
                msg += f'\nFont Size: <b>{fontsize}</b>'
                if fontcolour := self.extra_data.get('fontcolour'):
                    msg += f'\nFont Colour: <b>{fontcolour}</b>'
        if quality := self.extra_data.get('quality'):
            msg += f'\nQuality: <b>{quality}</b>'
        if self.mode == 'watermark' and (wmsize := self.extra_data.get('wmsize')):
            msg += f'\nWM Size: <b>{wmsize}</b>'
            if wmposition := self.extra_data.get('wmposition'):
                pos_dict = {'5:5': 'Top Left', 'main_w-overlay_w-5:5': 'Top Right', '5:main_h-overlay_h': 'Bottom Left', 'w-overlay_w-5:main_h-overlay_h-5': 'Bottom Right'}
                msg += f'\nWM Position: <b>{pos_dict[wmposition]}</b>'
            if popupwm := self.extra_data.get('popupwm'):
                msg += f'\nDisplay: <b>{popupwm}x/20s</b>'
        match mode:
            case 'rename':
                msg += '\n\n<i>Send valid name with extension...</i>'
            case 'watermark':
                msg += '\n\n<i>Send valid image to set as watermark...</i>'
            case 'subfile':
                msg += '\n\n<i>Send valid subtitle (.ass or .srt) for hardsub...</i>'
            case 'wmsize':
                msg += '\n\n<i>Choose watermark size</i>'
            case 'fontsize':
                msg += ('\n\n<i>Choose font size</i>\n'
                        '<b>Recommended:</b>\n'
                        '1080p: <b>21-26 </b>\n'
                        '720p: <b>16-21</b>\n'
                        '480p: <b>11-16</b>')
            case 'trim':
                msg += '\n\n<i>Send valid trim duration <b>hh:mm:ss hh:mm:ss</b></i>'
        msg += f'\n\n<i>Time Out: {get_readable_time(180 - (time() - self._time))}</i>'
        return msg

    async def list_buttons(self, mode: str=''):
        buttons, bnum = ButtonMaker(), 2
        if not mode:
            vid_modes = dict(list(VID_MODE.items())[4:]) if self._isLink else VID_MODE
            for key, value in vid_modes.items():
                buttons.button_data(f"{'🔵 ' if self.mode == key else ''}{value}", f'vidtool {key}')
            buttons.button_data(f'{"🔵 " if self.newname else ""}Rename', 'vidtool rename', 'header')
            buttons.button_data('Cancel', 'vidtool cancel', 'footer')
            if self.mode:
                buttons.button_data('Done', 'vidtool done', 'footer')
            if self.mode in ('vid_sub', 'watermark') and await CustomFilters.sudo('', self.listener.message):
                hardsub = self.extra_data.get('hardsub')
                buttons.button_data(f"{'🔵 ' if hardsub else ''}Hardsub", 'vidtool hardsub', 'header')
                if hardsub and self.mode == 'watermark':
                    buttons.button_data(f"{'🔵 ' if await aiopath.exists(self.extra_data.get('subfile', '')) else ''}Sub File", 'vidtool subfile', 'header')
                buttons.button_data('Font Style', 'vidtool fontstyle', 'header')
            if self.mode in ('compress', 'watermark') or self.extra_data.get('hardsub'):
                buttons.button_data('Quality', 'vidtool quality', 'header')
            if self.mode == 'watermark':
                buttons.button_data('Popup', 'vidtool popupwm', 'header')
        else:
            def _buttons_style(name=True, size=True, colour=True, position='header', cb='fontstyle'):
                if name:
                    buttons.button_data('Font Name', 'vidtool fontstyle fontname', position)
                if size:
                    buttons.button_data('Font Size', 'vidtool fontstyle fontsize', position)
                if colour:
                    buttons.button_data('Font Colour', 'vidtool fontstyle fontcolour', position)
                buttons.button_data('<<', f'vidtool {cb}', 'footer')
                buttons.button_data('Done', 'vidtool done', 'footer')

            match mode:
                case 'subsync':
                    buttons.button_data('Manual', 'vidtool sync_manual')
                    buttons.button_data('Auto', 'vidtool sync_auto')
                case 'quality':
                    bnum = 3
                    for key in ['1080p', '720p', '540p', '480p', '360p']:
                        buttons.button_data(f"{'🔵 ' if self.extra_data.get('quality') == key else ''}{key}", f'vidtool quality {key}')
                    buttons.button_data('<<', 'vidtool back', 'footer')
                    buttons.button_data('Done', 'vidtool done', 'footer')
                case 'popupwm':
                    bnum, popupwm = 5, self.extra_data.get('popupwm', 0)
                    if popupwm:
                        buttons.button_data('Reset', 'vidtool popupwm 0', 'header')
                    for key in range(2, 21, 2):
                        buttons.button_data(f"{'🔵 ' if popupwm == key else ''}{key}", f'vidtool popupwm {key}')
                    buttons.button_data('<<', 'vidtool back', 'footer')
                    buttons.button_data('Done', 'vidtool done', 'footer')
                case 'wmsize':
                    bnum = 3
                    for btn in [5, 10, 15, 20, 25, 30]:
                        buttons.button_data(str(btn), f'vidtool wmsize {btn}')
                case 'fontstyle':
                    bnum = 3
                    _buttons_style(position=None, cb='back')
                    buttons.button_data(f"{'🔵 ' if self.extra_data.get('boldstyle') else ''}Bold Style", f"vidtool fontstyle boldstyle {self.extra_data.get('boldstyle', False)}", 'header')
                case 'fontname':
                    _buttons_style(name=False)
                    for btn in ['Arial', 'Impact', 'Verdana', 'Consolas', 'DejaVu_Sans', 'Comic_Sans_MS', 'Simple_Day_Mistu']:
                        buttons.button_data(f"{'🔵 ' if btn == self.extra_data.get('fontname') else ''}{btn.replace('_', ' ')}", f'vidtool fontstyle fontname {btn}')
                case 'fontsize':
                    bnum = 5
                    _buttons_style(size=False)
                    for btn in range(11, 31):
                        buttons.button_data(f"{'🔵 ' if str(btn) == self.extra_data.get('fontsize') else ''}{btn}", f'vidtool fontstyle fontsize {btn}')
                case 'fontcolour':
                    bnum = 3
                    _buttons_style(colour=False)
                    colours = [('Red', '0000ff'), ('Green', '00ff00'), ('Blue', 'ff0000'), ('Yellow', '00ffff'), ('Orange', '0054ff'), ('Purple', '005aff')]
                    for btn, hexcolour in colours:
                        buttons.button_data(f"{'🔵 ' if hexcolour == self.extra_data.get('fontcolour') else ''}{btn}", f'vidtool fontstyle fontcolour {hexcolour}')
                case 'wmposition':
                    buttons.button_data('Top Left', 'vidtool wmposition 5:5')
                    buttons.button_data('Top Right', 'vidtool wmposition main_w-overlay_w-5:5')
                    buttons.button_data('Bottom Left', 'vidtool wmposition 5:main_h-overlay_h')
                    buttons.button_data('Bottom Right', 'vidtool wmposition w-overlay_w-5:main_h-overlay_h-5')
                case _:
                    buttons.button_data('<<', 'vidtool back', 'footer')

        await self._send_message(self._captions(mode), buttons.build_menu(bnum, 3))

    async def get_buttons(self):
        LOGGER.info(f"Starting get_buttons for user {self.listener.user_id}")
        future = self._event_handler()
        try:
            await gather(self.list_buttons(), wrap_future(future))
            if self.is_cancelled:
                await editMessage(self.mode, self._reply)
                LOGGER.info(f"Task cancelled for user {self.listener.user_id}")
                return None
            await deleteMessage(self._reply)
            LOGGER.info(f"Mode selected: {self.mode}, name: {self.newname}, extra_data: {self.extra_data}")
            return [self.mode, self.newname, self.extra_data]
        except Exception as e:
            LOGGER.error(f"Error in get_buttons: {e}", exc_info=True)
            return None

async def message_handler(_, message: Message, obj: SelectMode, is_sub=False):
    data = None
    if not message.text and not is_media(message):
        await sendMessage('Invalid input! Send text or media as required.', message)
        LOGGER.warning(f"Invalid input received from user {obj.listener.user_id}")
        return
    if obj.is_rename and message.text:
        obj.newname = message.text.strip().replace('/', '')
        obj.is_rename = False
        LOGGER.info(f"Name set to {obj.newname} by user {obj.listener.user_id}")
    elif obj.mode == 'watermark' and (media := is_media(message)):
        if is_sub:
            if message.document and not media.file_name.lower().endswith(('.ass', '.srt')):
                await sendMessage('Only .ass or .srt allowed!', message)
                LOGGER.warning(f"Invalid subtitle file from user {obj.listener.user_id}")
                return
            obj.extra_data['subfile'] = await message.download(ospath.join('watermark', media.file_id))
            LOGGER.info(f"Subtitle file downloaded: {obj.extra_data['subfile']}")
        else:
            if message.document and 'image' not in getattr(media, 'mime_type', 'None'):
                await sendMessage('Only image document allowed!', message)
                LOGGER.warning(f"Invalid watermark image from user {obj.listener.user_id}")
                return
            fpath = await message.download(ospath.join('watermark', media.file_id))
            try:
                await sync_to_async(Image.open(fpath).convert('RGBA').save, ospath.join('watermark', f'{obj.listener.mid}.png'), 'PNG')
                await clean_target(fpath)
                data = 'wmsize'
                LOGGER.info(f"Watermark image processed for user {obj.listener.user_id}")
            except Exception as e:
                LOGGER.error(f"Error processing watermark image: {e}")
                await clean_target(fpath)
                return
    elif obj.mode == 'trim' and message.text:
        if match := re_match(r'(\d{2}:\d{2}:\d{2})\s(\d{2}:\d{2}:\d{2})', message.text.strip()):
            obj.extra_data.update({'start_time': match.group(1), 'end_time': match.group(2)})
            LOGGER.info(f"Trim duration set: {obj.extra_data}")
        else:
            await sendMessage('Invalid trim duration format! Use hh:mm:ss hh:mm:ss', message)
            LOGGER.warning(f"Invalid trim duration format from user {obj.listener.user_id}")
            return
    obj.message_event.set()
    await gather(obj.list_buttons(data), deleteMessage(message))

@new_task
async def cb_vidtools(_, query: CallbackQuery, obj: SelectMode):
    data = query.data.split()
    if len(data) < 2:
        await query.answer("Invalid callback data!", show_alert=True)
        LOGGER.warning(f"Invalid callback data from user {obj.listener.user_id}: {query.data}")
        return
    if data[1] in config_dict['DISABLE_VIDTOOLS']:
        await query.answer(f'{VID_MODE[data[1]]} has been disabled!', show_alert=True)
        LOGGER.info(f"Disabled mode {data[1]} attempted by user {obj.listener.user_id}")
        return
    await query.answer()
    if data[1] == obj.mode and len(data) == 2:
        LOGGER.info(f"Redundant mode selection {data[1]} by user {obj.listener.user_id}")
        return
    LOGGER.info(f"Callback received: {query.data}")
    match data[1]:
        case 'done':
            obj.event.set()
            LOGGER.info(f"Done triggered by user {obj.listener.user_id}")
        case 'back':
            if obj.message_event.is_set():
                obj.message_event.clear()
            await obj.list_buttons()
            LOGGER.info(f"Back navigation by user {obj.listener.user_id}")
        case 'cancel':
            obj.mode = 'Task has been cancelled!'
            obj.is_cancelled = True
            obj.event.set()
            LOGGER.info(f"Cancel triggered by user {obj.listener.user_id}")
        case 'quality' | 'popupwm' as value:
            if len(data) == 3:
                obj.extra_data[value] = data[2] if value == 'quality' else int(data[2])
                LOGGER.info(f"{value} set to {data[2]} by user {obj.listener.user_id}")
            await obj.list_buttons(value)
        case 'hardsub':
            hmode = not bool(obj.extra_data.get('hardsub'))
            if not hmode and obj.mode == 'vid_sub':
                obj.extra_data.clear()
            obj.extra_data['hardsub'] = hmode
            LOGGER.info(f"Hardsub toggled to {hmode} by user {obj.listener.user_id}")
            await obj.list_buttons()
        case 'subfile':
            future = obj.message_event_handler('subfile')
            await gather(obj.list_buttons('subfile'), wrap_future(future))
            LOGGER.info(f"Subfile selection started by user {obj.listener.user_id}")
        case 'fontstyle':
            mode = 'fontstyle'
            if len(data) > 2:
                mode = data[2]
                is_bold = mode == 'boldstyle'
                if len(data) == 4:
                    if not is_bold and obj.extra_data.get(mode) == data[3]:
                        return
                    try:
                        obj.extra_data[mode] = not literal_eval(data[3]) if is_bold else data[3]
                        LOGGER.info(f"Fontstyle {mode} set to {obj.extra_data[mode]} by user {obj.listener.user_id}")
                    except ValueError:
                        LOGGER.error(f"Invalid boldstyle value: {data[3]}")
                        return
                if is_bold:
                    mode = 'fontstyle'
            await obj.list_buttons(mode)
        case 'sync_manual' | 'sync_auto' as value:
            obj.extra_data['type'] = value
            LOGGER.info(f"Subsync type set to {value} by user {obj.listener.user_id}")
            await obj.list_buttons()
        case 'wmsize' | 'wmposition' as value:
            if len(data) < 3:
                await query.answer(f"Missing value for {value}!", show_alert=True)
                LOGGER.warning(f"Missing value for {value} from user {obj.listener.user_id}")
                return
            obj.extra_data[value] = data[2]
            LOGGER.info(f"{value} set to {data[2]} by user {obj.listener.user_id}")
            await obj.list_buttons('wmposition' if value == 'wmsize' else None)
        case value:
            if value == 'rename':
                obj.is_rename = True
            else:
                obj.mode = value
                obj.extra_data.clear()
                LOGGER.info(f"Mode set to {value} by user {obj.listener.user_id}")
            if value in ['watermark', 'rename', 'trim']:
                future = obj.message_event_handler(value)
                await gather(obj.list_buttons(value), wrap_future(future))
                return
            await obj.list_buttons('subsync' if value == 'subsync' else '')