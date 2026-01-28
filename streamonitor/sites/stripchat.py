import itertools
import json
import os
import os.path
import random
import re
import unicodedata
import requests
import base64
import hashlib
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback for Python < 3.9
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        import pytz
        ZoneInfo = None

from streamonitor.bot import RoomIdBot
from streamonitor.downloaders.hls import getVideoNativeHLS
from streamonitor.enums import Status, Gender, COUNTRIES
from parameters import DOWNLOADS_DIR, CONTAINER


class StripChat(RoomIdBot):
    site = 'StripChat'
    siteslug = 'SC'

    bulk_update = True
    sleep_on_error = 10
    _static_data = None
    _mouflon_cache_filename = 'stripchat_mouflon_keys.json'
    _mouflon_keys: dict = None
    _cached_keys: dict[str, bytes] = None
    _PRIVATE_STATUSES = frozenset(["private", "groupShow", "p2p", "virtualPrivate", "p2pVoice"])
    _OFFLINE_STATUSES = frozenset(["off", "idle"])

    _GENDER_MAP = {
        'female': Gender.FEMALE,
        'male': Gender.MALE,
        'maleFemale': Gender.BOTH
    }

    if os.path.exists(_mouflon_cache_filename):
        with open(_mouflon_cache_filename) as f:
            try:
                if not isinstance(_mouflon_keys, dict):
                    _mouflon_keys = {}
                _mouflon_keys.update(json.load(f))
                print('Loaded StripChat mouflon key cache')
            except Exception as e:
                print('Error loading mouflon key cache:', e)

    def __init__(self, username, room_id=None):
        if StripChat._static_data is None:
            StripChat._static_data = {}
            try:
                self.getInitialData()
            except Exception as e:
                print('Error initializing StripChat static data:', e)

        super().__init__(username, room_id)
        self._id = None
        self.vr = False
        self.getVideo = lambda _, url, filename: getVideoNativeHLS(self, url, filename, StripChat.m3u_decoder)

    @classmethod
    def getInitialData(cls):
        session = requests.Session()
        r = session.get('https://stripchat.com/api/front/v3/config/static', headers=cls.headers)
        if r.status_code != 200:
            raise Exception("Failed to fetch static data from StripChat")
        StripChat._static_data = r.json().get('static')

    @classmethod
    def m3u_decoder(cls, content):
        _mouflon_filename = 'media.mp4'

        def _decode(encrypted_b64: str, key: str) -> str:
            if cls._cached_keys is None:
                cls._cached_keys = {}
            hash_bytes = cls._cached_keys[key] if key in cls._cached_keys \
                else cls._cached_keys.setdefault(key, hashlib.sha256(key.encode("utf-8")).digest())
            encrypted_data = base64.b64decode(encrypted_b64 + "==")
            return bytes(a ^ b for (a, b) in zip(encrypted_data, itertools.cycle(hash_bytes))).decode("utf-8")

        psch, pkey, pdkey = StripChat._getMouflonFromM3U(content)

        if psch == 'v1':
            _mouflon_file_attr = "#EXT-X-MOUFLON:FILE:"
        elif psch == 'v2':
            _mouflon_file_attr = "#EXT-X-MOUFLON:URI:"
        else:
            return None

        decoded = ''
        lines = content.splitlines()
        last_decoded_file = None
        for line in lines:
            if line.startswith(_mouflon_file_attr):
                if psch == 'v1':
                    last_decoded_file = _decode(line[len(_mouflon_file_attr):], pdkey)
                elif psch == 'v2':
                    uri = line[len(_mouflon_file_attr):]
                    encoded_part = uri.split('_')[-2]
                    decoded_part = _decode(encoded_part[::-1], pdkey)
                    last_decoded_file = uri.replace(encoded_part, decoded_part).split('/', maxsplit=4)[4]
            elif line.endswith(_mouflon_filename) and last_decoded_file:
                decoded += (line.replace(_mouflon_filename, last_decoded_file)) + '\n'
                last_decoded_file = None
            else:
                decoded += line + '\n'
        return decoded

    @classmethod
    def getMouflonDecKey(cls, pkey):
        if cls._mouflon_keys is None:
            cls._mouflon_keys = {}
        if pkey in cls._mouflon_keys:
            return cls._mouflon_keys[pkey]
        # else: find pdkey
        return None

    @staticmethod
    def _getMouflonFromM3U(m3u8_doc):
        _start = 0
        _needle = '#EXT-X-MOUFLON:'
        while _needle in (_doc := m3u8_doc[_start:]):
            _mouflon_start = _doc.find(_needle)
            if _mouflon_start > 0:
                _mouflon = _doc[_mouflon_start:m3u8_doc.find('\n', _mouflon_start)].strip().split(':')
                psch = _mouflon[2]
                pkey = _mouflon[3]
                pdkey = StripChat.getMouflonDecKey(pkey)
                if pdkey:
                    return psch, pkey, pdkey
            _start += _mouflon_start + len(_needle)
        return None, None, None

    def getWebsiteURL(self):
        return "https://stripchat.com/" + self.username

    @property
    def outputFolder(self):
        # New format: /{username} (only username, no platform)
        return str(os.path.join(DOWNLOADS_DIR, self.username))

    def genOutFilename(self, create_dir=True):
        folder = self.outputFolder
        if create_dir:
            os.makedirs(folder, exist_ok=True)
        
        # Get current time in Asia/Hong_Kong timezone with milliseconds
        if ZoneInfo is not None:
            # Use zoneinfo (Python 3.9+)
            hk_tz = ZoneInfo('Asia/Hong_Kong')
            now = datetime.now(hk_tz)
        else:
            # Fallback to pytz
            hk_tz = pytz.timezone('Asia/Hong_Kong')
            now = hk_tz.localize(datetime.now())
        
        # Format: YYYY-MM-DD-HHMMSS-milliseconds
        # Get milliseconds (microseconds / 1000)
        milliseconds = now.microsecond // 1000
        timestamp = now.strftime(f"%Y-%m-%d-%H%M%S-{milliseconds:03d}")
        
        # Get topic from lastInfo if available
        topic = None
        if hasattr(self, 'lastInfo') and self.lastInfo:
            topic = self.lastInfo.get('topic')
        
        # Clean topic for filename (remove invalid / problematic characters)
        if topic:
            # Remove or replace characters that are invalid in filenames
            # Windows: < > : " / \ | ? *
            # Unix: / (forward slash)
            # ! shell/history; ~ ～ and emoji can cause ffmpeg exit 254/183
            invalid_chars = r'[<>:"/\\|?*!\x00-\x1f~～]'
            topic = re.sub(invalid_chars, '_', str(topic))
            # Remove emoji / symbols / format chars that can break paths or make ffmpeg unable to re-open files
            # - So/Sk: emojis and other symbols
            # - Cf: zero-width / format chars (e.g. U+200B) that make filenames look identical but differ in bytes
            topic = ''.join(
                c for c in topic
                if unicodedata.category(c) not in ('So', 'Sk', 'Cf') and ord(c) < 0x10000
            )
            # Collapse whitespace to '_' to avoid odd unicode spaces in filenames
            topic = re.sub(r'\s+', '_', topic)
            # Collapse multiple underscores
            topic = re.sub(r'_+', '_', topic)
            # Remove leading/trailing spaces and dots
            topic = topic.strip(' ._')
            # Limit topic by UTF-8 byte length (80 bytes) so filename stays safe and avoids ffmpeg/path issues
            topic_utf8 = topic.encode('utf-8')
            if len(topic_utf8) > 80:
                topic = topic_utf8[:80].decode('utf-8', errors='ignore').strip(' ._')
            # If topic is empty after cleaning, set to None
            if not topic:
                topic = None
        
        # Build filename: {platform}-{username}-{timestamp}-{topic}.{container}
        # Format: StripChat-username-2025-01-26-143022-123-topic.mp4
        filename_parts = [self.site, self.username, timestamp]
        if topic:
            filename_parts.append(topic)
        
        filename = os.path.join(folder, '-'.join(filename_parts) + '.' + CONTAINER)
        
        return filename

    def getVideoUrl(self):
        return self.getWantedResolutionPlaylist(None)

    def getPlaylistVariants(self, url):
        url = "https://edge-hls.{host}/hls/{id}{vr}/master/{id}{vr}{auto}.m3u8".format(
                host='doppiocdn.' + random.choice(['org', 'com', 'net']),
                id=self.room_id,
                vr='_vr' if self.vr else '',
                auto='_auto' if not self.vr else ''
            )
        result = self.session.get(url, headers=self.headers, cookies=self.cookies)
        m3u8_doc = result.content.decode("utf-8")
        psch, pkey, pdkey = StripChat._getMouflonFromM3U(m3u8_doc)
        if pdkey is None:
            if pkey is not None:
                self.logger.warning(f'Failed to get mouflon decryption key for pkey={pkey} (psch={psch}). '
                                  f'Add this key to {self._mouflon_cache_filename} to enable decryption.')
                self.log(f'Failed to get mouflon decryption key (pkey: {pkey})')
            else:
                self.log(f'Failed to get mouflon decryption key (no mouflon tag found in playlist)')
            return []
        variants = super().getPlaylistVariants(m3u_data=m3u8_doc)
        return [variant | {'url': f'{variant["url"]}{"&" if "?" in variant["url"] else "?"}psch={psch}&pkey={pkey}'}
                for variant in variants]

    @staticmethod
    def uniq(length=16):
        chars = ''.join(chr(i) for i in range(ord('a'), ord('z')+1))
        chars += ''.join(chr(i) for i in range(ord('0'), ord('9')+1))
        return ''.join(random.choice(chars) for _ in range(length))

    def _getStatusData(self, username):
        try:
            r = self.session.get(
                f'https://stripchat.com/api/front/v2/models/username/{username}/cam?uniq={StripChat.uniq()}',
                headers=self.headers,
                timeout=15
            )
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            self.logger.warning('StripChat API unreachable (DNS/network): %s', e)
            return None
        except requests.exceptions.RequestException as e:
            self.logger.warning('StripChat API request failed: %s', e)
            return None

        try:
            data = r.json()
        except requests.exceptions.JSONDecodeError:
            self.log('Failed to parse JSON response')
            return None
        return data

    def _update_lastInfo(self, data):
        if data is None:
            return None
        if 'cam' not in data:
            if 'error' in data:
                error = data['error']
                if error == 'Not Found':
                    return Status.NOTEXIST
                self.logger.warn(f'Status returned error: {error}')
            return Status.UNKNOWN

        self.lastInfo = {'model': data['user']['user']}
        if isinstance(data['cam'], dict):
            self.lastInfo |= data['cam']
        return None

    def getRoomIdFromUsername(self, username):
        if username == self.username and self.room_id is not None:
            return self.room_id

        data = self._getStatusData(username)
        if username == self.username:
            self._update_lastInfo(data)

        if 'user' not in data:
            return None
        if 'user' not in data['user']:
            return None
        if 'id' not in data['user']['user']:
            return None

        return str(data['user']['user']['id'])

    def getStatus(self):
        data = self._getStatusData(self.username)
        if data is None:
            return Status.UNKNOWN

        error = self._update_lastInfo(data)
        if error:
            return error

        if 'user' in data and 'user' in data['user']:
            model_data = data['user']['user']
            if model_data.get('gender'):
                self.gender = StripChat._GENDER_MAP.get(model_data.get('gender'))

            if model_data.get('country'):
                self.country = model_data.get('country', '').upper()
            elif model_data.get('languages'):
                for lang in model_data['languages']:
                    if lang.upper() in COUNTRIES:
                        self.country = lang.upper()
                        break

        status = self.lastInfo['model'].get('status')
        if status == "public" and self.lastInfo["isCamAvailable"] and self.lastInfo["isCamActive"]:
            return Status.PUBLIC
        if status in self._PRIVATE_STATUSES:
            return Status.PRIVATE
        if status in self._OFFLINE_STATUSES:
            return Status.OFFLINE
        if self.lastInfo['model'].get('isDeleted') is True:
            return Status.NOTEXIST
        if data['user'].get('isGeoBanned') is True:
            return Status.RESTRICTED
        self.logger.warn(f'Got unknown status: {status}')
        return Status.UNKNOWN

    @classmethod
    def getStatusBulk(cls, streamers):
        model_ids = {}
        for streamer in streamers:
            if not isinstance(streamer, StripChat):
                continue
            if streamer.room_id:
                model_ids[streamer.room_id] = streamer

        base_url = 'https://stripchat.com/api/front/models/list?'
        batch_num = 100
        data_map = {}
        model_id_list = list(model_ids)
        for _batch_ids in [model_id_list[i:i+batch_num] for i in range(0, len(model_id_list), batch_num)]:
            session = requests.Session()
            session.headers.update(cls.headers)
            r = session.get(base_url + '&'.join(f'modelIds[]={model_id}' for model_id in _batch_ids), timeout=10)

            try:
                data = r.json()
            except requests.exceptions.JSONDecodeError:
                print('Failed to parse JSON response')
                return
            data_map |= {str(model['id']): model for model in data.get('models', [])}

        for model_id, streamer in model_ids.items():
            model_data = data_map.get(model_id)
            if not model_data:
                streamer.setStatus(Status.UNKNOWN)
                continue
            if model_data.get('gender'):
                streamer.gender = cls._GENDER_MAP.get(model_data.get('gender'))
            if model_data.get('country'):
                streamer.country = model_data.get('country', '').upper()
            status = model_data.get('status')
            if status == "public" and model_data.get("isOnline"):
                streamer.setStatus(Status.PUBLIC)
            elif status in cls._PRIVATE_STATUSES:
                streamer.setStatus(Status.PRIVATE)
            elif status in cls._OFFLINE_STATUSES:
                streamer.setStatus(Status.OFFLINE)
            else:
                print(f'[{streamer.siteslug}] {streamer.username}: Bulk update got unknown status: {status}')
                streamer.setStatus(Status.UNKNOWN)
