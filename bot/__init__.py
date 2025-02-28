from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aria2p import API as ariaAPI, Client as ariaClient
from asyncio import Lock
from base64 import b64decode
from dotenv import load_dotenv, dotenv_values
from logging import getLogger, FileHandler, StreamHandler, basicConfig, INFO, ERROR, warning as log_warning
from os import remove as osremove, path as ospath, environ, getcwd
from pymongo import MongoClient
from pyrogram import Client as tgClient, __version__
from pyrogram.enums import ParseMode
from qbittorrentapi import Client as qbClient
from re import sub as resub
from socket import setdefaulttimeout
from subprocess import Popen, run as srun, check_output
from sys import exit
from time import sleep, time
from tzlocal import get_localzone
from uvloop import install


# from faulthandler import enable as faulthandler_enable
# faulthandler_enable()
install()
setdefaulttimeout(600)

botStartTime = time()

getLogger('qbittorrentapi').setLevel(INFO)
getLogger('requests').setLevel(INFO)
getLogger('urllib3').setLevel(INFO)
getLogger('pyrogram').setLevel(ERROR)
getLogger('googleapiclient.discovery').setLevel(ERROR)
getLogger('httpx').setLevel(ERROR)

basicConfig(format='%(asctime)s: [%(levelname)s: %(filename)s - %(lineno)d] ~ %(message)s',
            handlers=[FileHandler('log.txt'), StreamHandler()],
            datefmt='%d-%b-%y %I:%M:%S %p',
            level=INFO)


LOGGER = getLogger(__name__)

aria2 = ariaAPI(ariaClient(host='http://localhost', port=6800, secret=''))

load_dotenv('config.env', override=True)

Intervals = {'status': {}, 'qb': '', 'jd': ''}
QbTorrents = {}
jd_downloads = {}
DRIVES_NAMES = []
DRIVES_IDS = []
INDEX_URLS = []
SHORTENERES = []
SHORTENER_APIS = []
GLOBAL_EXTENSION_FILTER = ['aria2', '!qB']
user_data = {}
aria2_options = {}
qbit_options = {}
queued_dl = {}
queued_up = {}
non_queued_dl = set()
non_queued_up = set()
multi_tags = set()

task_dict_lock = Lock()
queue_dict_lock = Lock()
qb_listener_lock = Lock()
jd_lock = Lock()
cpu_eater_lock = Lock()
subprocess_lock = Lock()
bot_lock = Lock()
status_dict = {}
task_dict = {}
rss_dict = {}
bot_dict = {}

VID_MODE = {'vid_vid': 'Video + Video',
            'vid_aud': 'Video + Audio',
            'vid_sub': 'Video + Subtitle',
            'subsync': 'SubSync',
            'compress': 'Compress',
            'convert': 'Convert',
            'watermark': 'Watermark',
            'extract': 'Extract',
            'trim': 'Trim',
            'rmstream': 'Remove Stream',
            'merge_rmaudio': 'Merge and Remove Audio',
            'merge_preremove_audio': 'Merge with Pre-Remove Audio'}

DEFAULT_SPLIT_SIZE = 2097151000
ARIA_NAME = environ.get('ARIA_NAME', 'aria2c')
QBIT_NAME = environ.get('QBIT_NAME', 'qbittorrent-nox')
FFMPEG_NAME = environ.get('FFMPEG_NAME', 'ffmpeg')

# ============================ REQUIRED ================================
if not (BOT_TOKEN := environ.get('BOT_TOKEN', '6499364659:AAHMmUxMWag28I9V_9YJBi8qaZWZ0VstGEk')):
    LOGGER.error('BOT_TOKEN variable is missing! Exiting now')
    exit(1)

bot_id = BOT_TOKEN.split(':', 1)[0]

if DATABASE_URL := environ.get('DATABASE_URL', 'mongodb+srv://hello:hello@cluster0.vc2htx0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'):
    if not DATABASE_URL.startswith('mongodb'):
        try:
            DATABASE_URL = b64decode(resub('ini|adalah|pesan|yang|sangat|rahasia', '', DATABASE_URL)).decode('utf-8')
        except:
            pass
    try:
        conn = MongoClient(DATABASE_URL)
        db = conn.mltb
        current_config = dict(dotenv_values('config.env'))
        old_config = db.settings.deployConfig.find_one({'_id': bot_id})
        if old_config is None:
            db.settings.deployConfig.replace_one({'_id': bot_id}, current_config, upsert=True)
        else:
            del old_config['_id']
        if old_config and old_config != current_config:
            db.settings.deployConfig.replace_one({'_id': bot_id}, current_config, upsert=True)
        elif config_dict := db.settings.config.find_one({'_id': bot_id}):
            del config_dict['_id']
            for key, value in config_dict.items():
                environ[key] = str(value)
        if pf_dict := db.settings.files.find_one({'_id': bot_id}):
            del pf_dict['_id']
            for key, value in pf_dict.items():
                if value:
                    file_ = key.replace('__', '.')
                    LOGGER.info('%s has been impoerted from database.', file_)
                    with open(file_, 'wb+') as f:
                        f.write(value)
                    if file_ == 'cfg.zip':
                        srun(['rm', '-rf', '/JDownloader/cfg'])
                        srun(['7z', 'x', 'cfg.zip', '-o/JDownloader'])
                        osremove('cfg.zip')
        if a2c_options := db.settings.aria2c.find_one({'_id': bot_id}):
            del a2c_options['_id']
            aria2_options = a2c_options
            LOGGER.info('Aria2c settings imported from database.')
        if qbit_opt := db.settings.qbittorrent.find_one({'_id': bot_id}):
            del qbit_opt['_id']
            qbit_options = qbit_opt
            LOGGER.info('QBittorrent settings imported from database.')
        conn.close()
        BOT_TOKEN = environ.get('BOT_TOKEN', '7588796776:AAFO4NRS0ptkUr8yMHUxdg8Kgy82B0ZcUlQ')
        bot_id = BOT_TOKEN.split(':', 1)[0]
        if DATABASE_URL := environ.get('DATABASE_URL', 'mongodb+srv://hello:hello@cluster0.vc2htx0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'):
            if not DATABASE_URL.startswith('mongodb'):
                try:
                    DATABASE_URL = b64decode(resub('ini|adalah|pesan|rahasia', '', DATABASE_URL)).decode('utf-8')
                except:
                    pass
    except Exception as e:
        LOGGER.error('Database ERROR: %s', e)
else:
    config_dict = {}

if OWNER_ID := environ.get('OWNER_ID', '1596559467'):
    OWNER_ID = int(OWNER_ID)
else:
    LOGGER.error('OWNER_ID variable is missing! Exiting now')
    exit(1)

if TELEGRAM_API := environ.get('TELEGRAM_API', '4857766'):
    TELEGRAM_API = int(TELEGRAM_API)
else:
    LOGGER.error('TELEGRAM_API variable is missing! Exiting now')
    exit(1)

if not (TELEGRAM_HASH := environ.get('TELEGRAM_HASH', '6c3c6facf5598a4b318e138f8c407028')):
    LOGGER.error('TELEGRAM_HASH variable is missing! Exiting now')
    exit(1)

DOWNLOAD_DIR = environ.get('DOWNLOAD_DIR', '/usr/src/app/downloads/')
if not DOWNLOAD_DIR.endswith('/'):
    DOWNLOAD_DIR += '/'

GDRIVE_ID = environ.get('GDRIVE_ID', '0AOIjN1u1lhaiUk9PVA')
CLOUD_LINK_FILTERS = environ.get('CLOUD_LINK_FILTERS', 'mypikpak.com')
RCLONE_PATH = environ.get('RCLONE_PATH', 'MAIN:DL')
RCLONE_FLAGS = environ.get('RCLONE_FLAGS', '')

DEFAULT_UPLOAD = environ.get('DEFAULT_UPLOAD', 'rc')
if DEFAULT_UPLOAD != 'rc':
    DEFAULT_UPLOAD = 'gd'
# ======================================================================


# =========================== OPTIONALS ===============================
if AUTHORIZED_CHATS := environ.get('AUTHORIZED_CHATS', ''):
    aid = AUTHORIZED_CHATS.split()
    for id_ in aid:
        user_data[int(id_.strip())] = {'is_auth': True}

if SUDO_USERS := environ.get('SUDO_USERS', ''):
    aid = SUDO_USERS.split()
    for id_ in aid:
        user_data[int(id_.strip())] = {'is_sudo': True}

if EXTENSION_FILTER := environ.get('EXTENSION_FILTER', ''):
    fx = EXTENSION_FILTER.split()
    for x in fx:
        x = x.lstrip('.')
        GLOBAL_EXTENSION_FILTER.append(x.strip().lower())

TORRENT_TIMEOUT = environ.get('TORRENT_TIMEOUT', '')
TORRENT_TIMEOUT = int(TORRENT_TIMEOUT) if TORRENT_TIMEOUT else ''
            
QUEUE_ALL = environ.get('QUEUE_ALL', '')
QUEUE_ALL = int(QUEUE_ALL) if QUEUE_ALL else ''

QUEUE_DOWNLOAD = environ.get('QUEUE_DOWNLOAD', '5')
QUEUE_DOWNLOAD = int(QUEUE_DOWNLOAD) if QUEUE_DOWNLOAD else ''

QUEUE_UPLOAD = environ.get('QUEUE_UPLOAD', '')
QUEUE_UPLOAD = int(QUEUE_UPLOAD) if QUEUE_UPLOAD else ''

ARGO_TOKEN = environ.get('ARGO_TOKEN', '')
PING_URL = environ.get('PING_URL', '')
ENABLE_STREAM_LINK = environ.get('ENABLE_STREAM_LINK', 'False').lower() == 'true'
STREAM_BASE_URL = environ.get('STREAM_BASE_URL', '').rstrip('/')
STREAM_PORT = environ.get('STREAM_PORT', '')
QUEUE_COMPLETE = environ.get('QUEUE_COMPLETE', 'True').lower() == 'true'
DISABLE_MIRROR_LEECH = environ.get('DISABLE_MIRROR_LEECH', '')
INDEX_URL = environ.get('INDEX_URL', '').rstrip('/')
INCOMPLETE_TASK_NOTIFIER = environ.get('INCOMPLETE_TASK_NOTIFIER', 'True').lower() == 'true'
INCOMPLETE_AUTO_RESUME = environ.get('INCOMPLETE_AUTO_RESUME', 'True').lower() == 'true'
USE_SERVICE_ACCOUNTS = environ.get('USE_SERVICE_ACCOUNTS', 'False').lower() == 'true'
CMD_SUFFIX = environ.get('CMD_SUFFIX', '')
DATABASE_URL = environ.get('DATABASE_URL', 'mongodb+srv://hello:hello@cluster0.vc2htx0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
AUTO_THUMBNAIL = environ.get('AUTO_THUMBNAIL', 'True').lower() == 'true'
PREMIUM_MODE = environ.get('PREMIUM_MODE', 'True').lower() == 'true'
SESSION_TIMEOUT = int(environ.get('SESSION_TIMEOUT', 0))
DAILY_MODE = environ.get('DAILY_MODE', 'False').lower() == 'true'
MEDIA_GROUP = environ.get('MEDIA_GROUP', 'False').lower() == 'true'
STOP_DUPLICATE = environ.get('STOP_DUPLICATE', 'True').lower() == 'true'
IS_TEAM_DRIVE = environ.get('IS_TEAM_DRIVE', 'True').lower() == 'true'
MULTI_TIMEGAP = int(environ.get('MULTI_TIMEGAP', 5))
AS_DOCUMENT = environ.get('AS_DOCUMENT', 'False').lower() == 'true'
SAVE_MESSAGE = environ.get('SAVE_MESSAGE', 'True').lower() == 'true'
LEECH_FILENAME_PREFIX = environ.get('LEECH_FILENAME_PREFIX', '')
LEECH_INFO_PIN = environ.get('LEECH_INFO_PIN', 'False').lower() == 'true'
USER_SESSION_STRING = environ.get('USER_SESSION_STRING', '')
SAVE_SESSION_STRING = environ.get('SAVE_SESSION_STRING', '')
USERBOT_LEECH = environ.get('USERBOT_LEECH', 'False').lower() == 'true'
AUTO_DELETE_MESSAGE_DURATION = int(environ.get('AUTO_DELETE_MESSAGE_DURATION', 30))
AUTO_DELETE_UPLOAD_MESSAGE_DURATION = int(environ.get('AUTO_DELETE_UPLOAD_MESSAGE_DURATION', 30))
STATUS_UPDATE_INTERVAL = int(environ.get('STATUS_UPDATE_INTERVAL', 5))
YT_DLP_OPTIONS = environ.get('YT_DLP_OPTIONS', '')
DAILY_LIMIT_SIZE = int(environ.get('DAILY_LIMIT_SIZE', 50))
COMPRESS_BANNER = environ.get('COMPRESS_BANNER', 'Re-Encoded')
LIB264_PRESET = environ.get('LIB264_PRESET', 'superfast')
LIB265_PRESET = environ.get('LIB265_PRESET', 'faster')
HARDSUB_FONT_SIZE = environ.get('HARDSUB_FONT_SIZE', '20')
HARDSUB_FONT_NAME = environ.get('HARDSUB_FONT_NAME', 'Simple Day Mistu')
VIDTOOLS_FAST_MODE = environ.get('VIDTOOLS_FAST_MODE', 'False').lower() == 'true'
DISABLE_VIDTOOLS = environ.get('DISABLE_VIDTOOLS', 'None')
DISABLE_MULTI_VIDTOOLS = environ.get('DISABLE_MULTI_VIDTOOLS', 'None')
START_MESSAGE = environ.get('START_MESSAGE', '')
# ======================================================================


# ============================= RCLONE =================================
ENABLE_FASTDL = environ.get('ENABLE_FASTDL', 'False').lower() == 'true'
RCLONE_SERVE_URL = environ.get('RCLONE_SERVE_URL', '').rstrip('/')
RCLONE_SERVE_PORT = environ.get('RCLONE_SERVE_PORT', '')
RCLONE_SERVE_USER = environ.get('RCLONE_SERVE_USER', '')
RCLONE_SERVE_PASS = environ.get('RCLONE_SERVE_PASS', '')
RCLONE_TFSIMULATION = int(environ.get('RCLONE_TFSIMULATION', '8'))
# ======================================================================
                
            
# ============================== LOGS ==================================
ONCOMPLETE_LEECH_LOG = environ.get('ONCOMPLETE_LEECH_LOG', 'True').lower() == 'true'
LEECH_LOG = environ.get('LEECH_LOG', '-1001963446260')
if LEECH_LOG:
    if LEECH_LOG.isdigit() or LEECH_LOG.startswith('-'):
        LEECH_LOG = int(LEECH_LOG)
else:
    ENABLE_STREAM_LINK = False

MIRROR_LOG = environ.get('MIRROR_LOG', '-1001963446260')
MIRROR_LOG = int(MIRROR_LOG) if MIRROR_LOG.isdigit() or MIRROR_LOG.startswith('-') else MIRROR_LOG

OTHER_LOG = environ.get('OTHER_LOG', '-1001963446260')
OTHER_LOG = int(OTHER_LOG) if OTHER_LOG.isdigit() or OTHER_LOG.startswith('-') else OTHER_LOG

LINK_LOG = environ.get('LINK_LOG', '-1001963446260')
LINK_LOG = int(LINK_LOG) if LINK_LOG.isdigit() or LINK_LOG.startswith('-') else LINK_LOG
# ======================================================================


# ============================= LIMITS =================================
EQUAL_SPLITS = environ.get('EQUAL_SPLITS', 'False').lower() == 'true'

CLONE_LIMIT = ''

LEECH_LIMIT = environ.get('LEECH_LIMIT', '')
LEECH_LIMIT = float(LEECH_LIMIT) if LEECH_LIMIT else ''

LEECH_SPLIT_SIZE = environ.get('LEECH_SPLIT_SIZE', '')
LEECH_SPLIT_SIZE = int(LEECH_SPLIT_SIZE) if LEECH_SPLIT_SIZE else ''

MEGA_LIMIT = environ.get('MEGA_LIMIT', '')
MEGA_LIMIT = float(MEGA_LIMIT) if MEGA_LIMIT else ''

NONPREMIUM_LIMIT = environ.get('NONPREMIUM_LIMIT', '5')
NONPREMIUM_LIMIT = float(NONPREMIUM_LIMIT) if NONPREMIUM_LIMIT else ''

STATUS_LIMIT = environ.get('STATUS_LIMIT', '')
STATUS_LIMIT = int(STATUS_LIMIT) if STATUS_LIMIT else 5

TORRENT_DIRECT_LIMIT = environ.get('TORRENT_DIRECT_LIMIT', '')
TORRENT_DIRECT_LIMIT = float(TORRENT_DIRECT_LIMIT) if TORRENT_DIRECT_LIMIT else ''
FSUB_CHANNEL_ID = "-1001963446260"
TOTAL_TASKS_LIMIT = environ.get('TOTAL_TASKS_LIMIT', '')
TOTAL_TASKS_LIMIT = int(TOTAL_TASKS_LIMIT) if TOTAL_TASKS_LIMIT else ''

MEGA_EMAIL = environ.get('MEGA_EMAIL', '')
MEGA_PASSWORD = environ.get('MEGA_PASSWORD', '')
if len(MEGA_EMAIL) == 0 or len(MEGA_PASSWORD) == 0:
    log_warning('MEGA Credentials not provided!')
    MEGA_EMAIL = ''
    MEGA_PASSWORD = ''
            
USER_TASKS_LIMIT = environ.get('USER_TASKS_LIMIT', '')
USER_TASKS_LIMIT = int(USER_TASKS_LIMIT) if USER_TASKS_LIMIT else ''

ZIP_UNZIP_LIMIT = environ.get('ZIP_UNZIP_LIMIT', '')
ZIP_UNZIP_LIMIT = float(ZIP_UNZIP_LIMIT) if ZIP_UNZIP_LIMIT else ''

STORAGE_THRESHOLD = environ.get('STORAGE_THRESHOLD', '')
STORAGE_THRESHOLD = float(STORAGE_THRESHOLD) if STORAGE_THRESHOLD else ''

MAX_YTPLAYLIST = environ.get('MAX_YTPLAYLIST', '')
MAX_YTPLAYLIST = int(MAX_YTPLAYLIST) if MAX_YTPLAYLIST else ''
# ======================================================================


# ============================= GOFILE =================================
GOFILE = environ.get('GOFILE', 'False').lower() == 'true'
GOFILETOKEN = environ.get('GOFILETOKEN', '')
GOFILEBASEFOLDER = environ.get('GOFILEBASEFOLDER', '')
if not GOFILETOKEN or not GOFILEBASEFOLDER:
    GOFILE = False
if GOFILE:
    LOGGER.info('GoFile feature has been enabled!')
# ======================================================================


# ============================= FORCE =================================
# Auto Mute
FORCE_SHORTEN = environ.get('FORCE_SHORTEN', 'False').lower() == 'true'
AUTO_MUTE = environ.get('AUTO_MUTE', 'False').lower() == 'true'
MUTE_CHAT_ID = "-1001963446260"
AUTO_MUTE_DURATION = int(environ.get('AUTO_MUTE_DURATION', 30))
# Username
FUSERNAME = environ.get('FUSERNAME', 'False').lower() == 'true'
# Subscribe
FSUB = environ.get('FSUB', 'False').lower() == 'true'
#FSUB_CHANNEL_ID = int(environ.get('FSUB_CHANNEL_ID', ''))
FSUB_BUTTON_NAME = environ.get('FSUB_BUTTON_NAME', 'Join Channel')
CHANNEL_USERNAME = environ.get('CHANNEL_USERNAME', 'hexafreinds')
# ======================================================================


# ============================ STICKERS ================================
STICKERID_COUNT = environ.get('STICKERID_COUNT', 'CAACAgQAAxkBAAKE5GcY_waybDI9-OZqyB11EXYdoGrHAAKiGgACIPZSDMoAAcpM2WzqZDYE')
STICKERID_ERROR = environ.get('STICKERID_ERROR', 'CAACAgUAAxkBAAKE0mcY-4dK5n6f6g48N9Ewan6EwvqzAAKiBwACo5GpVIbD3xPwDzqSNgQ')
STICKERID_LEECH = environ.get('STICKERID_LEECH', 'CAACAgQAAxkBAAKE4WcY_ZUB7seTjXokfngVI1d-8rOnAAKxGgACIPZSDOnpobqbU-_dNgQ')
STICKERID_MIRROR = environ.get('STICKERID_MIRROR', 'CAACAgUAAxkBAAKE3mcY_RIiOQABU057pFcXzGirQawrNwACXQ8AAhNcSFbQs6XZtrORmzYE')
STICKER_DELETE_DURATION = int(environ.get('STICKER_DELETE_DURATION', 120))
# ======================================================================


# ============================ IMAGES ==================================
images = 'https://graph.org/file/79d62e05db6894c34f0df-c54d163cde4785ca28.jpg https://graph.org/file/16464195c37e1255985d5-a380149a9cf3a9c661.jpg https://graph.org/file/c831753c2850d6d0809e5-521f7b72e9c08672f9.jpg \
          https://graph.org/file/4d60d42b9dac0896abea8-7a4ccf2a93912bf049.jpg https://graph.org/file/30686e1ae6cef2fa823c1-bc4a51ea5e2ce1782c.jpg https://graph.org/file/f123aaa392a8fe38d0b52-b602d462af2a664099.jpg \
          https://graph.org/file/bd39056e99c5d8e781883-870eed8cc4f0104788.jpg https://graph.org/file/9da9ad2e1fe22049d4118-ce8e6720aa35c3de67.jpg https://graph.org/file/44353a4dcb7b88cec6434-4701e8b86e5aeec010.jpg'
ENABLE_IMAGE_MODE = environ.get('ENABLE_IMAGE_MODE', 'True').lower() == 'true'
IMAGE_ARIA = environ.get('IMAGE_ARIA', 'https://graph.org/file/b955fe4acb20149ba4868-309fb913aa926b75dd.jpg')
IMAGE_AUTH = environ.get('IMAGE_AUTH', 'https://graph.org/file/0cf8c787d792a6f04791b-ac5532767649d372c1.jpg')
IMAGE_BOLD = environ.get('IMAGE_BOLD', 'https://graph.org/file/e0c29cf9bbf7dfd5d852e-82b1e7ff72aaf399e6.jpg')
IMAGE_BYE = environ.get('IMAGE_BYE', 'https://graph.org/file/67b2460f8876fc32db886-c89db46da6662af053.jpg')
IMAGE_CANCEL = environ.get('IMAGE_CANCEL', 'https://graph.org/file/ba0a2e0fe939b05427a92-6bfb6342485bf0e53a.jpg')
IMAGE_CAPTION = environ.get('IMAGE_CAPTION', 'https://graph.org/file/2da667f5f93737fa5ee70-7db40690a107384a39.jpg')
IMAGE_COMMONS_CHECK = environ.get('IMAGE_COMMONS_CHECK', 'https://graph.org/file/518f52a70c871909fbff3-aabf1f8cb7901658fe.jpg')
IMAGE_COMPLETE = environ.get('IMAGE_COMPLETE', images)
IMAGE_CONEDIT = environ.get('IMAGE_CONEDIT', 'https://graph.org/file/54dcbd2bc0698c571e2d9-b590c6c0a96f575f29.jpg')
IMAGE_CONPRIVATE = environ.get('IMAGE_CONPRIVATE', 'https://graph.org/file/aced58062006da3dab2a6-f9a4fd26303a6c584d.jpg')
IMAGE_CONSET = environ.get('IMAGE_CONSET', 'https://graph.org/file/671bdaeef5dd5a948f8a6-0329ac606444dd6b55.jpg')
IMAGE_CONVIEW = environ.get('IMAGE_CONVIEW', 'https://graph.org/file/7ebb7d1b137c22576e586-658e0c588d8314b696.jpg')
IMAGE_DUMP = environ.get('IMAGE_DUMP', 'https://graph.org/file/fedd4776a5c358b9c1ad2-45cfa75bf17cd052b9.jpg')
IMAGE_EXTENSION = environ.get('IMAGE_EXTENSION', 'https://graph.org/file/241538f6ca7c53befafad-695cf5ae06ed1c83d3.jpg')
IMAGE_GD = environ.get('IMAGE_GD', 'https://graph.org/file/834e49f9b5b31985dc63e-0ddbfe0c0af13b289f.jpg')
IMAGE_HELP = environ.get('IMAGE_HELP', 'https://graph.org/file/786d876956ffd92933e5a-d7d3374cd21780c4cb.jpg')
IMAGE_HTML = environ.get('IMAGE_HTML', 'https://graph.org/file/11dc20f036a5725ebb04d-5b2067f4882df86a2e.jpg')
IMAGE_IMDB = environ.get('IMAGE_IMDB', 'https://graph.org/file/69d0b2a3da7daaca13374-878f19d5458afec3bf.jpg')
IMAGE_INFO = environ.get('IMAGE_INFO', 'https://graph.org/file/5ef00ebe4fff7bf441be0-7c0dd7c0af27b4f553.jpg')
IMAGE_ITALIC = environ.get('IMAGE_ITALIC', 'https://graph.org/file/f8184418b1f921a43a7de-a320e7db583217b0ea.jpg')
IMAGE_JD = environ.get('IMAGE_JD', 'https://graph.org/file/90d9e5281227509960d73-d5b0d7a77fcc82b2b8.jpg')
IMAGE_LOGS = environ.get('IMAGE_LOGS', 'https://graph.org/file/3d3d4ff6da76a7ba9ae7e-d79bc00f3e921a022f.jpg')
IMAGE_MDL = environ.get('IMAGE_MDL', 'https://graph.org/file/bf022a38e6e604314bcc0-b52c3c257d5c95634a.jpg')
IMAGE_MEDINFO = environ.get('IMAGE_MEDINFO', 'https://graph.org/file/01d4c038e7ffafa2d7b01-ad60baee9fa8647c44.jpg')
IMAGE_METADATA = environ.get('IMAGE_METADATA', 'https://graph.org/file/9257f155ec587cc708e03-452d86c7775776f5d4.jpg')
IMAGE_MONO = environ.get('IMAGE_MONO', 'https://graph.org/file/1136d3236f65922811cf5-305a6d4fc7bbbd73bc.jpg')
IMAGE_NORMAL = environ.get('IMAGE_NORMAL', 'https://graph.org/file/d959822219c176e3f8c8e-408da582f04a89f846.jpg')
IMAGE_OWNER = environ.get('IMAGE_OWNER', 'https://graph.org/file/2748a01faa27b6a6877e1-d6fe36073bd672ab8d.jpg')
IMAGE_PAUSE = environ.get('IMAGE_PAUSE', 'https://graph.org/file/5274b0b22d50f70d4ac5c-be94eff5048ac8a959.jpg')
IMAGE_PRENAME = environ.get('IMAGE_PRENAME', 'https://graph.org/file/36389155d2eab8fbbe8b8-559521f36cfe5b00ea.jpg')
IMAGE_QBIT = environ.get('IMAGE_QBIT', 'https://graph.org/file/06c06767756761a69ef40-17797558c5cdc21d1a.jpg')
IMAGE_RCLONE = environ.get('IMAGE_RCLONE', 'https://graph.org/file/6b6e4b9fd02e60bee148a-a091906157e2454f0f.jpg')
IMAGE_REMNAME = environ.get('IMAGE_REMNAME', 'https://graph.org/file/4d10c8f4248daf4a490aa-c320435d4408f54385.jpg')
IMAGE_RSS = environ.get('IMAGE_RSS', 'https://graph.org/file/d9b8562c5c130dc4f7a14-794534955e3faa34a5.jpg')
IMAGE_SEARCH = environ.get('IMAGE_SEARCH', 'https://graph.org/file/d27aa5734d1661de17f39-c639d6df08e87eb95d.jpg')
IMAGE_STATS = environ.get('IMAGE_STATS', 'https://graph.org/file/fa1e0859b156b522a7a0b-6a200457fa30020594.jpg')
IMAGE_STATUS = environ.get('IMAGE_STATUS', 'https://graph.org/file/267a5d4ed16bd826cc0ad-28109866401cfeb6e0.jpg')
IMAGE_SUFNAME = environ.get('IMAGE_SUFNAME', 'https://graph.org/file/9934d97e31a71332b1643-0f222759a97146e877.jpg')
IMAGE_TMDB = environ.get('IMAGE_TMDB', 'https://graph.org/file/a7cabcbcf13eba509dbaa-87b76e87a199561b28.jpg')
IMAGE_TXT = environ.get('IMAGE_TXT', 'https://graph.org/file/63f9023443fe2102274d5-811a7178da9a5f63c6.jpg')
IMAGE_UNAUTH = environ.get('IMAGE_UNAUTH', 'https://envs.sh/Ah0.jpg')
IMAGE_UNKNOW = environ.get('IMAGE_UNKNOW', 'https://envs.sh/AhI.jpg')
IMAGE_USER = environ.get('IMAGE_USER', 'https://envs.sh/Ahn.jpg')
IMAGE_USETIINGS = environ.get('IMAGE_USETIINGS', 'https://envs.sh/AhT.jpg')
IMAGE_VIDTOOLS = environ.get('IMAGE_VIDTOOLS', 'https://envs.sh/AhR.jpg')
IMAGE_WEL = environ.get('IMAGE_WEL', 'https://envs.sh/AhJ.jpg')
IMAGE_WIBU = environ.get('IMAGE_WIBU', 'https://envs.sh/AhH.jpg')
IMAGE_YT = environ.get('IMAGE_YT', 'https://envs.sh/AhV.jpg')
IMAGE_ZIP = environ.get('IMAGE_ZIP', 'https://envs.sh/Add.jpg')
# ======================================================================


# =========================== ACCOUNTS =================================
# JDownloader
JD_EMAIL = environ.get('JD_EMAIL', '')
JD_PASS = environ.get('JD_PASS', '')
# Uptobox
UPTOBOX_TOKEN = environ.get('UPTOBOX_TOKEN', '')
# GDTot
CRYPT_GDTOT = environ.get('CRYPT_GDTOT', '')
# FileLion
FILELION_API = environ.get('FILELION_API', '')
# StreamWish
STREAMWISH_API = environ.get('STREAMWISH_API', '')
# SharerPW
SHARERPW_LARAVEL_SESSION = environ.get('SHARERPW_LARAVEL_SESSION', '')
SHARERPW_XSRF_TOKEN = environ.get('SHARERPW_XSRF_TOKEN', '')
# ======================================================================


# =========================== UPSTREAM =================================
UPSTREAM_REPO = environ.get('UPSTREAM_REPO', '')
UPSTREAM_BRANCH = environ.get('UPSTREAM_BRANCH', 'master')
UPDATE_EVERYTHING = environ.get('UPDATE_EVERYTHING', 'False').lower() == 'true'
# ======================================================================


# ============================== UI ====================================
AUTHOR_NAME = environ.get('AUTHOR_NAME', 'Maheshsirop')
AUTHOR_URL = environ.get('AUTHOR_URL', 'https://t.me/maheshsirop')
DRIVE_SEARCH_TITLE = environ.get('DRIVE_SEARCH_TITLE', 'Drive Search')
GD_INFO = environ.get('GD_INFO', 'By @maheshsirop')
PROG_FINISH = environ.get('PROG_FINISH', '⬤')
PROG_UNFINISH = environ.get('PROG_UNFINISH', '○')
SOURCE_LINK_TITLE = environ.get('SOURCE_LINK_TITLE', 'Source Link')
TIME_ZONE = environ.get('TIME_ZONE', 'Asia/Kolkata')
TIME_ZONE_TITLE = environ.get('TIME_ZONE_TITLE', 'GMT+05:30')
TSEARCH_TITLE = environ.get('TSEARCH_TITLE', 'Torrent Search')
# ======================================================================


# =========================== BUTTONS =================================
SOURCE_LINK = environ.get('SOURCE_LINK', 'True').lower() == 'true'
VIEW_LINK = environ.get('VIEW_LINK', 'True').lower() == 'true'
BUTTON_FIVE_NAME = environ.get('BUTTON_FIVE_NAME', '')
BUTTON_FIVE_URL = environ.get('BUTTON_FIVE_URL', '')
BUTTON_FOUR_NAME = environ.get('BUTTON_FOUR_NAME', '')
BUTTON_FOUR_URL = environ.get('BUTTON_FOUR_URL', '')
BUTTON_SIX_NAME = environ.get('BUTTON_SIX_NAME', '')
BUTTON_SIX_URL = environ.get('BUTTON_SIX_URL', '')
# ======================================================================


# =========================== QBITTORRENT ==============================
BASE_URL_PORT = environ.get('BASE_URL_PORT', '')
BASE_URL_PORT = 80 if len(BASE_URL_PORT) == 0 else int(BASE_URL_PORT)

BASE_URL = environ.get('BASE_URL', '').rstrip("/")
if len(BASE_URL) == 0:
    BASE_URL = ''

PORT = environ.get('PORT', '')
PORT = 80 if len(PORT) == 0 else int(PORT)

            
TORRENT_PORT = environ.get('TORRENT_PORT', '5000')
WEB_PINCODE = environ.get('WEB_PINCODE', 'False').lower() == 'true'
# ======================================================================


# =============================== RSS ==================================
RSS_CHAT = environ.get('RSS_CHAT', '')
RSS_CHAT = int(RSS_CHAT) if RSS_CHAT.isdigit() or RSS_CHAT.startswith('-') else RSS_CHAT

RSS_DELAY = environ.get('RSS_DELAY', '')
RSS_DELAY = int(RSS_DELAY) if RSS_DELAY else 900
# ======================================================================


# ============================ TORSEARCH ===============================
SEARCH_LIMIT = int(environ.get('SEARCH_LIMIT', 20))
SEARCH_API_LINK = environ.get('SEARCH_API_LINK', '').rstrip('/')
SEARCH_PLUGINS = environ.get('SEARCH_PLUGINS', '')
# ======================================================================


# ============================= HEROKU =================================
HEROKU_API_KEY = environ.get('HEROKU_API_KEY', '')
HEROKU_APP_NAME = environ.get('HEROKU_APP_NAME', '')
# ======================================================================

IS_PREMIUM = None

config_dict = {'BOT_TOKEN': BOT_TOKEN,
               'BASE_URL': BASE_URL,
               'BASE_URL_PORT': BASE_URL_PORT,
               'TELEGRAM_API': TELEGRAM_API,
               'TELEGRAM_HASH': TELEGRAM_HASH,
               'OWNER_ID': OWNER_ID,
               'DATABASE_URL': DATABASE_URL,
               'DOWNLOAD_DIR': DOWNLOAD_DIR,
               'GDRIVE_ID': GDRIVE_ID,
               'CLOUD_LINK_FILTERS': CLOUD_LINK_FILTERS,
               'DEFAULT_UPLOAD': DEFAULT_UPLOAD,
               # OPTIONALS
               'ARGO_TOKEN': ARGO_TOKEN,
               'PING_URL': PING_URL,
               'TORRENT_PORT': TORRENT_PORT,
               'START_MESSAGE': START_MESSAGE,
               'COMPRESS_BANNER': COMPRESS_BANNER,
               'LIB264_PRESET': LIB264_PRESET,
               'LIB265_PRESET': LIB265_PRESET,
               'HARDSUB_FONT_NAME': HARDSUB_FONT_NAME,
               'HARDSUB_FONT_SIZE': HARDSUB_FONT_SIZE,
               'VIDTOOLS_FAST_MODE': VIDTOOLS_FAST_MODE,
               'DISABLE_VIDTOOLS': DISABLE_VIDTOOLS,
               'DISABLE_MULTI_VIDTOOLS': DISABLE_MULTI_VIDTOOLS,
               'ENABLE_STREAM_LINK': ENABLE_STREAM_LINK,
               'STREAM_BASE_URL': STREAM_BASE_URL,
               'STREAM_PORT': STREAM_PORT,
               'DISABLE_MIRROR_LEECH': DISABLE_MIRROR_LEECH,
               'AUTHORIZED_CHATS': AUTHORIZED_CHATS,
               'SUDO_USERS': SUDO_USERS,
               'EXTENSION_FILTER': EXTENSION_FILTER,
               'INDEX_URL': INDEX_URL,
               'MEGA_EMAIL': MEGA_EMAIL,
               'MEGA_PASSWORD': MEGA_PASSWORD,
               'TORRENT_TIMEOUT': TORRENT_TIMEOUT,
               'INCOMPLETE_TASK_NOTIFIER': INCOMPLETE_TASK_NOTIFIER,
               'INCOMPLETE_AUTO_RESUME': INCOMPLETE_AUTO_RESUME,
               'USE_SERVICE_ACCOUNTS': USE_SERVICE_ACCOUNTS,
               'CMD_SUFFIX': CMD_SUFFIX,
               'STOP_DUPLICATE': STOP_DUPLICATE,
               'IS_TEAM_DRIVE': IS_TEAM_DRIVE,
               'MULTI_TIMEGAP': MULTI_TIMEGAP,
               'AS_DOCUMENT': AS_DOCUMENT,
               'SAVE_MESSAGE': SAVE_MESSAGE,
               'LEECH_FILENAME_PREFIX': LEECH_FILENAME_PREFIX,
               'LEECH_INFO_PIN': LEECH_INFO_PIN,
               'USER_SESSION_STRING': USER_SESSION_STRING,
               'SAVE_SESSION_STRING': SAVE_SESSION_STRING,
               'USERBOT_LEECH': USERBOT_LEECH,
               'AUTO_DELETE_MESSAGE_DURATION': AUTO_DELETE_MESSAGE_DURATION,
               'AUTO_DELETE_UPLOAD_MESSAGE_DURATION': AUTO_DELETE_UPLOAD_MESSAGE_DURATION,
               'STATUS_UPDATE_INTERVAL': STATUS_UPDATE_INTERVAL,
               'YT_DLP_OPTIONS': YT_DLP_OPTIONS,
               'AUTO_THUMBNAIL': AUTO_THUMBNAIL,
               'PREMIUM_MODE': PREMIUM_MODE,
               'SESSION_TIMEOUT': SESSION_TIMEOUT,
               'DAILY_MODE': DAILY_MODE,
               'MEDIA_GROUP': MEDIA_GROUP,
               'QUEUE_ALL': QUEUE_ALL,
               'QUEUE_DOWNLOAD': QUEUE_DOWNLOAD,
               'QUEUE_UPLOAD': QUEUE_UPLOAD,
               'QUEUE_COMPLETE': QUEUE_COMPLETE,
               # RCLONE
               'ENABLE_FASTDL': ENABLE_FASTDL,
               'RCLONE_FLAGS': RCLONE_FLAGS,
               'RCLONE_PATH': RCLONE_PATH,
               'RCLONE_SERVE_URL': RCLONE_SERVE_URL,
               'RCLONE_SERVE_PORT': RCLONE_SERVE_PORT,
               'RCLONE_SERVE_USER': RCLONE_SERVE_USER,
               'RCLONE_SERVE_PASS': RCLONE_SERVE_PASS,
               'RCLONE_TFSIMULATION': RCLONE_TFSIMULATION,
               # LOGS
               'ONCOMPLETE_LEECH_LOG': ONCOMPLETE_LEECH_LOG,
               'LEECH_LOG': LEECH_LOG,
               'MIRROR_LOG': MIRROR_LOG,
               'OTHER_LOG': OTHER_LOG,
               'LINK_LOG': LINK_LOG,
               # LIMITS
               'EQUAL_SPLITS': EQUAL_SPLITS,
               'DAILY_LIMIT_SIZE': DAILY_LIMIT_SIZE,
               'CLONE_LIMIT': CLONE_LIMIT,
               'LEECH_LIMIT': LEECH_LIMIT,
               'LEECH_SPLIT_SIZE': LEECH_SPLIT_SIZE,
               'MEGA_LIMIT': MEGA_LIMIT,
               'NONPREMIUM_LIMIT': NONPREMIUM_LIMIT,
               'STATUS_LIMIT': STATUS_LIMIT,
               'TORRENT_DIRECT_LIMIT': TORRENT_DIRECT_LIMIT,
               'TOTAL_TASKS_LIMIT': TOTAL_TASKS_LIMIT,
               'USER_TASKS_LIMIT': USER_TASKS_LIMIT,
               'ZIP_UNZIP_LIMIT': ZIP_UNZIP_LIMIT,
               'STORAGE_THRESHOLD': STORAGE_THRESHOLD,
               'MAX_YTPLAYLIST': MAX_YTPLAYLIST,
               # GOFILE
               'GOFILE': GOFILE,
               'GOFILETOKEN': GOFILETOKEN,
               'GOFILEBASEFOLDER': GOFILEBASEFOLDER,
               # FMODE
               'FORCE_SHORTEN': FORCE_SHORTEN,
               'AUTO_MUTE': AUTO_MUTE,
               'MUTE_CHAT_ID': MUTE_CHAT_ID,
               'AUTO_MUTE_DURATION': AUTO_MUTE_DURATION,
               'FUSERNAME': FUSERNAME,
               'FSUB': FSUB,
               'FSUB_CHANNEL_ID': FSUB_CHANNEL_ID,
               'FSUB_BUTTON_NAME': FSUB_BUTTON_NAME,
               'CHANNEL_USERNAME': CHANNEL_USERNAME,
               # STICKERS
               'STICKER_DELETE_DURATION': STICKER_DELETE_DURATION,
               'STICKERID_COUNT': STICKERID_COUNT,
               'STICKERID_ERROR': STICKERID_ERROR,
               'STICKERID_LEECH': STICKERID_LEECH,
               'STICKERID_MIRROR': STICKERID_MIRROR,
               # IMAGES
               'ENABLE_IMAGE_MODE': ENABLE_IMAGE_MODE,
               'IMAGE_ARIA': IMAGE_ARIA,
               'IMAGE_AUTH': IMAGE_AUTH,
               'IMAGE_BOLD': IMAGE_BOLD,
               'IMAGE_BYE': IMAGE_BYE,
               'IMAGE_CANCEL': IMAGE_CANCEL,
               'IMAGE_CAPTION': IMAGE_CAPTION,
               'IMAGE_COMPLETE': IMAGE_COMPLETE,
               'IMAGE_CONEDIT': IMAGE_CONEDIT,
               'IMAGE_CONPRIVATE': IMAGE_CONPRIVATE,
               'IMAGE_CONSET': IMAGE_CONSET,
               'IMAGE_CONVIEW': IMAGE_CONVIEW,
               'IMAGE_DUMP': IMAGE_DUMP,
               'IMAGE_EXTENSION': IMAGE_EXTENSION,
               'IMAGE_GD': IMAGE_GD,
               'IMAGE_HELP': IMAGE_HELP,
               'IMAGE_HTML': IMAGE_HTML,
               'IMAGE_IMDB': IMAGE_IMDB,
               'IMAGE_INFO': IMAGE_INFO,
               'IMAGE_ITALIC': IMAGE_ITALIC,
               'IMAGE_JD': IMAGE_JD,
               'IMAGE_LOGS': IMAGE_LOGS,
               'IMAGE_MDL': IMAGE_MDL,
               'IMAGE_MEDINFO': IMAGE_MEDINFO,
               'IMAGE_METADATA': IMAGE_METADATA,
               'IMAGE_MONO': IMAGE_MONO,
               'IMAGE_NORMAL': IMAGE_NORMAL,
               'IMAGE_OWNER': IMAGE_OWNER,
               'IMAGE_PAUSE': IMAGE_PAUSE,
               'IMAGE_PRENAME': IMAGE_PRENAME,
               'IMAGE_QBIT': IMAGE_QBIT,
               'IMAGE_RCLONE': IMAGE_RCLONE,
               'IMAGE_REMNAME': IMAGE_REMNAME,
               'IMAGE_RSS': IMAGE_RSS,
               'IMAGE_SEARCH': IMAGE_SEARCH,
               'IMAGE_STATS': IMAGE_STATS,
               'IMAGE_STATUS': IMAGE_STATUS,
               'IMAGE_SUFNAME': IMAGE_SUFNAME,
               'IMAGE_TMDB': IMAGE_TMDB,
               'IMAGE_TXT': IMAGE_TXT,
               'IMAGE_UNAUTH': IMAGE_UNAUTH,
               'IMAGE_UNKNOW': IMAGE_UNKNOW,
               'IMAGE_USER': IMAGE_USER,
               'IMAGE_USETIINGS': IMAGE_USETIINGS,
               'IMAGE_VIDTOOLS': IMAGE_VIDTOOLS,
               'IMAGE_WEL': IMAGE_WEL,
               'IMAGE_WIBU': IMAGE_WIBU,
               'IMAGE_YT': IMAGE_YT,
               'IMAGE_ZIP': IMAGE_ZIP,
               'IMAGE_COMMONS_CHECK': IMAGE_COMMONS_CHECK,
               # ACCOUNTS
               'JD_EMAIL': JD_EMAIL,
               'JD_PASS': JD_PASS,
               'UPTOBOX_TOKEN': UPTOBOX_TOKEN,
               'CRYPT_GDTOT': CRYPT_GDTOT,
               'FILELION_API': FILELION_API,
               'STREAMWISH_API': STREAMWISH_API,
               'SHARERPW_LARAVEL_SESSION': SHARERPW_LARAVEL_SESSION,
               'SHARERPW_XSRF_TOKEN': SHARERPW_XSRF_TOKEN,
               # UPSTREAM
               'UPSTREAM_REPO': UPSTREAM_REPO,
               'UPSTREAM_BRANCH': UPSTREAM_BRANCH,
               'UPDATE_EVERYTHING': UPDATE_EVERYTHING,
               # UI
               'AUTHOR_NAME': AUTHOR_NAME,
               'AUTHOR_URL': AUTHOR_URL,
               'DRIVE_SEARCH_TITLE': DRIVE_SEARCH_TITLE,
               'GD_INFO': GD_INFO,
               'PROG_FINISH': PROG_FINISH ,
               'PROG_UNFINISH': PROG_UNFINISH ,
               'SOURCE_LINK_TITLE': SOURCE_LINK_TITLE,
               'TIME_ZONE': TIME_ZONE,
               'TIME_ZONE_TITLE': TIME_ZONE_TITLE,
               'TSEARCH_TITLE': TSEARCH_TITLE,
               # BUTTONS
               'SOURCE_LINK': SOURCE_LINK,
               'VIEW_LINK': VIEW_LINK,
               'BUTTON_FIVE_NAME': BUTTON_FIVE_NAME,
               'BUTTON_FIVE_URL': BUTTON_FIVE_URL,
               'BUTTON_FOUR_NAME': BUTTON_FOUR_NAME,
               'BUTTON_FOUR_URL': BUTTON_FOUR_URL,
               'BUTTON_SIX_NAME': BUTTON_SIX_NAME,
               'BUTTON_SIX_URL': BUTTON_SIX_URL,
               # QBITTORRENT
               'BASE_URL': BASE_URL,
               'WEB_PINCODE': WEB_PINCODE,
               # RSS
               'RSS_CHAT': RSS_CHAT,
               'RSS_DELAY': RSS_DELAY,
               # TORSEARCH
               'SEARCH_API_LINK': SEARCH_API_LINK,
               'SEARCH_PLUGINS': SEARCH_PLUGINS,
               'SEARCH_LIMIT': SEARCH_LIMIT,
               # HEROKU
               'HEROKU_API_KEY': HEROKU_API_KEY,
               'HEROKU_APP_NAME': HEROKU_APP_NAME}

if GDRIVE_ID:
    DRIVES_NAMES.append('Main')
    DRIVES_IDS.append(GDRIVE_ID)
    INDEX_URLS.append(INDEX_URL)

if ospath.exists('list_drives.txt'):
    with open('list_drives.txt', 'r+') as f:
        lines = f.readlines()
        for line in lines:
            temp = line.strip().split()
            DRIVES_IDS.append(temp[1])
            DRIVES_NAMES.append(temp[0].replace('_', ' '))
            if len(temp) > 2:
                INDEX_URLS.append(temp[2])
            else:
                INDEX_URLS.append('')

if ospath.exists('shorteners.txt'):
    with open('shorteners.txt', 'r+') as f:
        lines = f.readlines()
        for line in lines:
            temp = line.strip().split()
            if len(temp) == 2:
                SHORTENERES.append(temp[0])
                SHORTENER_APIS.append(temp[1])


if not config_dict['ARGO_TOKEN']:
    config_dict['TORRENT_PORT'] = PORT
            
PORT = environ.get('PORT')
Popen(f"gunicorn web.wserver:app --bind 0.0.0.0:{PORT} --worker-class gevent", shell=True)

srun([QBIT_NAME, '-d', f'--profile={getcwd()}'], check=True)
if not ospath.exists('.netrc'):
    with open('.netrc', 'w'):
        pass
srun('chmod 600 .netrc && cp .netrc /root/.netrc', shell=True)
trackers = check_output("curl -Ns https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/all.txt https://ngosang.github.io/trackerslist/trackers_all_http.txt https://newtrackon.com/api/all https://raw.githubusercontent.com/hezhijie0327/Trackerslist/main/trackerslist_tracker.txt | awk '$0' | tr '\n\n' ','", shell=True).decode('utf-8').rstrip(',')
with open('a2c.conf', 'a+') as a:
    if TORRENT_TIMEOUT:
        a.write(f'bt-stop-timeout={TORRENT_TIMEOUT}\n')
    a.write(f'bt-tracker=[{trackers}]')
srun([ARIA_NAME, f'--conf-path={ospath.join(getcwd(), "a2c.conf")}'], check=True)
alive = Popen(["python3", "alive.py"])
sleep(0.5)
if ospath.exists('accounts.zip'):
    if ospath.exists('accounts'):
        srun(['rm', '-rf', 'accounts'], check=True)
    srun('7z x -o. -aoa accounts.zip accounts/*.json && chmod -R 777 accounts', shell=True)
    osremove('accounts.zip')
if not ospath.exists('accounts'):
    config_dict['USE_SERVICE_ACCOUNTS'] = False


def get_client():
    return qbClient(host='localhost', port=8090, VERIFY_WEBUI_CERTIFICATE=False, REQUESTS_ARGS={'timeout': (30, 60)})


aria2c_global = ['bt-max-open-files', 'download-result', 'keep-unfinished-download-result', 'log', 'log-level', 'max-concurrent-downloads', 'max-download-result',
                 'max-overall-download-limit', 'save-session', 'max-overall-upload-limit', 'optimize-concurrent-downloads', 'save-cookies', 'server-stat-of']

qb_client = get_client()
if not qbit_options:
    qbit_options = dict(qb_client.app_preferences())
    del qbit_options['listen_port']
    for k in list(qbit_options.keys()):
        if k.startswith('rss'):
            del qbit_options[k]
else:
    qb_opt = {**qbit_options}
    for k, v in list(qb_opt.items()):
        if v in ['', '*']:
            del qb_opt[k]
    qb_client.app_set_preferences(qb_opt)

LOGGER.info('Creating client Pyrofork V%s...', __version__)
kwargs = {'workers': 1000,  'parse_mode': ParseMode.HTML}
if int(__version__.replace('.', '')[:3]) > 221:
    kwargs.update({'max_concurrent_transmissions': 1000})
bot: tgClient = tgClient('bot', TELEGRAM_API, TELEGRAM_HASH, bot_token=BOT_TOKEN, **kwargs).start()

bot_loop = bot.loop
bot_name = bot.me.username
scheduler = AsyncIOScheduler(timezone=str(get_localzone()), event_loop=bot_loop)

if not aria2_options:
    aria2_options = aria2.client.get_global_option()
else:
    a2c_glo = {op: aria2_options[op] for op in aria2c_global if op in aria2_options}
    aria2.set_global_options(a2c_glo)
