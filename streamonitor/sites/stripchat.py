import itertools
import json
import os.path
import random
import re
import requests
import base64
import hashlib
import m3u8
from urllib.parse import urljoin

from streamonitor.bot import RoomIdBot
from streamonitor.downloaders.hls import getVideoNativeHLS
from streamonitor.enums import Status, Gender, COUNTRIES
from parameters import STRIPCHAT_COOKIE, STRIPCHAT_PREFER_AV1, STRIPCHAT_PREFER_FMP4, WANTED_RESOLUTION, WANTED_RESOLUTION_PREFERENCE


class StripChat(RoomIdBot):
    site = 'StripChat'
    siteslug = 'SC'

    bulk_update = True
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
        if STRIPCHAT_COOKIE:
            self.headers['Cookie'] = STRIPCHAT_COOKIE
            self.session.headers.update({'Cookie': STRIPCHAT_COOKIE})
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

    def getVideoUrl(self):
        sources = self.getPlaylistVariants(None)
        if not sources:
            self.logger.error("No available sources")
            return None

        selected_source = self._select_preferred_variant(sources)
        if selected_source is None:
            self.logger.error("Couldn't select a resolution")
            return None

        width, height = selected_source['resolution']
        frame_rate = selected_source.get('frame_rate')
        codecs = selected_source.get('codecs') or 'unknown codec'
        stream_type = 'fMP4' if selected_source.get('is_fmp4') else 'HLS'
        if height != 0:
            frame_rate_text = f" {frame_rate}fps" if frame_rate not in (None, 0) else ''
            self.logger.info(f"Selected {width}x{height}{frame_rate_text} [{codecs}] ({stream_type})")
        else:
            self.logger.info(f"Selected source [{codecs}] ({stream_type})")
        return selected_source['url']

    @staticmethod
    def _is_av1_variant(source):
        codecs = (source.get('codecs') or '').lower()
        return 'av01' in codecs or 'av1' in codecs

    def _inspect_variant_playlist(self, variant):
        if 'is_fmp4' in variant:
            return variant

        inspected_variant = dict(variant)
        inspected_variant['is_fmp4'] = False
        try:
            playlist_url = urljoin(variant.get('master_url'), variant['url'])
            result = self.session.get(playlist_url, headers=self.headers, cookies=self.cookies, timeout=10)
            playlist_text = result.content.decode("utf-8")
            decoded_text = StripChat.m3u_decoder(playlist_text)
            if decoded_text is not None:
                playlist_text = decoded_text
            playlist = m3u8.loads(playlist_text)
            inspected_variant['is_fmp4'] = len(playlist.segment_map) > 0 or any(
                segment.uri.endswith(('.m4s', '.mp4', '.cmfv', '.cmfa'))
                for segment in playlist.segments
            )
        except Exception as e:
            self.debug(f'Failed to inspect variant playlist: {e}')

        return inspected_variant

    @staticmethod
    def _select_source_for_resolution(sources):
        sources = [dict(source) for source in sources]
        for source in sources:
            width, height = source['resolution']
            if width < height:
                source['resolution_diff'] = width - WANTED_RESOLUTION
            else:
                source['resolution_diff'] = height - WANTED_RESOLUTION

        sources.sort(key=lambda a: abs(a['resolution_diff']))
        selected_source = None

        if WANTED_RESOLUTION_PREFERENCE == 'exact':
            if sources[0]['resolution_diff'] == 0:
                selected_source = sources[0]
        elif WANTED_RESOLUTION_PREFERENCE == 'closest' or len(sources) == 1:
            selected_source = sources[0]
        elif WANTED_RESOLUTION_PREFERENCE == 'exact_or_least_higher':
            for source in sources:
                if source['resolution_diff'] >= 0:
                    selected_source = source
                    break
        elif WANTED_RESOLUTION_PREFERENCE == 'exact_or_highest_lower':
            for source in sources:
                if source['resolution_diff'] <= 0:
                    selected_source = source
                    break
        else:
            return None

        return selected_source

    def _select_preferred_variant(self, sources):
        inspected_sources = sources
        if STRIPCHAT_PREFER_FMP4:
            inspected_sources = [self._inspect_variant_playlist(source) for source in inspected_sources]

        preferred_sources = inspected_sources
        if STRIPCHAT_PREFER_AV1:
            av1_sources = [source for source in preferred_sources if self._is_av1_variant(source)]
            if av1_sources:
                preferred_sources = av1_sources
                self.debug('Preferring AV1 StripChat variant')

        if STRIPCHAT_PREFER_FMP4:
            fmp4_sources = [source for source in preferred_sources if source.get('is_fmp4')]
            if fmp4_sources:
                preferred_sources = fmp4_sources
                self.debug('Preferring fMP4 StripChat variant')

        selected_source = self._select_source_for_resolution(preferred_sources)
        if selected_source is not None:
            return selected_source

        if preferred_sources is not inspected_sources:
            self.logger.warning('Preferred StripChat variant was not available at the requested resolution, falling back')
            return self._select_source_for_resolution(inspected_sources)

        return None

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
            self.log(f'Failed to get mouflon decryption key')
            return []
        variants = super().getPlaylistVariants(m3u_data=m3u8_doc)
        return [variant | {
                    'url': f'{variant["url"]}{"&" if "?" in variant["url"] else "?"}psch={psch}&pkey={pkey}',
                    'master_url': url
                }
                for variant in variants]

    @staticmethod
    def uniq(length=16):
        chars = ''.join(chr(i) for i in range(ord('a'), ord('z')+1))
        chars += ''.join(chr(i) for i in range(ord('0'), ord('9')+1))
        return ''.join(random.choice(chars) for _ in range(length))

    def _getStatusData(self, username):
        r = self.session.get(
            f'https://stripchat.com/api/front/v2/models/username/{username}/cam?uniq={StripChat.uniq()}',
            headers=self.headers,
            cookies=self.cookies
        )

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
