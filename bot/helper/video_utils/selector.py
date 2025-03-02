from __future__ import annotations
from asyncio import Event, wait_for, wrap_future, gather
from functools import partial
from pyrogram.filters import regex, user
from pyrogram.handlers import CallbackQueryHandler
from pyrogram.types import CallbackQuery
from time import time

from bot import VID_MODE, LOGGER
from bot.helper.ext_utils.bot_utils import new_task, new_thread
from bot.helper.listeners import tasks_listener as task
from bot.helper.telegram_helper.button_build import ButtonMaker
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, deleteMessage
from bot.helper.ext_utils.status_utils import get_readable_time

class SelectMode:
    def __init__(self, listener: task.TaskListener, isLink=False):
        self._isLink = isLink
        self._time = time()
        self._reply = None
        self.listener = listener
        self.mode = ''
        self.newname = ''
        self.event = Event()
        self.is_cancelled = False
        LOGGER.info(f"Initialized SelectMode for user {self.listener.user_id}, isLink: {isLink}")

    @new_thread
    async def _event_handler(self):
        LOGGER.info(f"Starting SelectMode event handler for user {self.listener.user_id}")
        pfunc = partial(cb_vidtools, obj=self)
        handler = None
        try:
            handler = self.listener.client.add_handler(
                CallbackQueryHandler(pfunc, filters=regex('^vidtool') & user(self.listener.user_id)), group=-1)
            await wait_for(self.event.wait(), timeout=180)
            LOGGER.info(f"SelectMode event completed for user {self.listener.user_id}")
        except TimeoutError:
            self.mode = 'Task has been cancelled due to timeout!'
            self.is_cancelled = True
            self.event.set()
            LOGGER.warning(f"SelectMode timed out for user {self.listener.user_id}")
        except Exception as e:
            LOGGER.error(f"Event handler error in SelectMode: {str(e)}", exc_info=True)
            self.is_cancelled = True
            self.event.set()
        finally:
            if handler:
                self.listener.client.remove_handler(*handler)
            LOGGER.info(f"Event handler finished for user {self.listener.user_id}")

    async def _send_message(self, text: str, buttons):
        try:
            if not self._reply:
                self._reply = await sendMessage(text, self.listener.message, buttons)
                LOGGER.info(f"Sent initial message for mode selection to user {self.listener.user_id}")
            else:
                await editMessage(text, self._reply, buttons)
                LOGGER.info(f"Updated message for mode selection for user {self.listener.user_id}")
        except Exception as e:
            LOGGER.error(f"Failed to send message: {e}", exc_info=True)

    def _captions(self):
        msg = (f'<b>VIDEO TOOLS SETTINGS</b>\n'
               f'Mode: <b>{VID_MODE.get(self.mode, "Not Selected")}</b>\n'
               f'Output Name: <b>{self.newname or "Default"}</b>')
        elapsed_time = int(time() - self._time)
        remaining_time = max(0, 180 - elapsed_time)
        msg += f'\n\n<i>Time Remaining: {get_readable_time(remaining_time)}</i>'
        return msg

    async def list_buttons(self):
        buttons = ButtonMaker()
        modes = {'merge_rmaudio': 'Merge & Remove Audio', 'merge_preremove_audio': 'Merge Pre-Remove Audio'}
        for key, value in modes.items():
            buttons.button_data(f"{'🔵 ' if self.mode == key else ''}{value}", f'vidtool {key}')
        buttons.button_data('Cancel', 'vidtool cancel', 'footer')
        if self.mode:
            buttons.button_data('Done', 'vidtool done', 'footer')
        await self._send_message(self._captions(), buttons.build_menu(2))

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
            LOGGER.info(f"Mode selected: {self.mode}, name: {self.newname}")
            return [self.mode, self.newname, {}]
        except Exception as e:
            LOGGER.error(f"Error in get_buttons: {e}", exc_info=True)
            return None

@new_task
async def cb_vidtools(_, query: CallbackQuery, obj: SelectMode):
    data = query.data.split()
    if len(data) < 2:
        await query.answer("Invalid callback data!", show_alert=True)
        return
    await query.answer()
    LOGGER.info(f"Callback received: {query.data}")
    match data[1]:
        case 'done':
            obj.event.set()
            LOGGER.info(f"Done triggered by user {obj.listener.user_id}")
        case 'cancel':
            obj.mode = 'Task has been cancelled!'
            obj.is_cancelled = True
            obj.event.set()
            LOGGER.info(f"Cancel triggered by user {obj.listener.user_id}")
        case value:
            obj.mode = value
            LOGGER.info(f"Mode set to {value} by user {obj.listener.user_id}")
            await obj.list_buttons()