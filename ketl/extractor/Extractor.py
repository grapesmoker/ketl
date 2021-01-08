import urllib.parse as up
from collections import namedtuple
from datetime import datetime
from ftplib import FTP
from functools import partial
from pathlib import Path
from typing import List

from furl import furl
from smart_open import open as smart_open
from tqdm import tqdm

from ketl.db.models import API, FileCache
from ketl.db.settings import get_session

# from src.network.apis import BaseAPI


SourceTargetPair = namedtuple('SourceTargetPair', ['source', 'target'])


class BaseExtractor:

    BLOCK_SIZE = 16384

    def __init__(self, api_config: API, show_progress=False):

        self.api = api_config
        self.headers = {}
        self.auth = None
        self.auth_token = None

        if self.api.creds:
            details = self.api.creds.creds_details
            cookie = details.get('cookie', None)
            if cookie:
                self.headers['Cookie'] = cookie['name'] + '=' + cookie['value']
            self.auth = details.get('auth', None)
            self.auth_token = details.get('auth_token', None)
            if self.auth_token:
                self.headers[self.auth_token['header']] = self.auth_token['token']

    @property
    def source_target_list(self):

        return [SourceTargetPair(
            source=source_file,
            target=Path(source.data_dir).resolve().
                joinpath(
                    source_file.path or furl(source_file.url).path.segments[-1].lstrip('/')
                ))
                for source in self.api.sources
                for source_file in source.source_files]  # type: FileCache

    def download_files(self) -> List[Path]:

        result = filter(None, [self.get_file(st_pair.source, st_pair.target, show_progress=True)
                               for st_pair in self.source_target_list])

        return list(result)

    @classmethod
    def _fetch_ftp_file(cls, url: str, target_file: Path, show_progress=False, force_download=False):

        parsed_url = up.urlparse(url)
        ftp = FTP(parsed_url.hostname)
        ftp.login()
        total_size = ftp.size(parsed_url.path)

        if cls._requires_update(target_file, total_size, 7) or force_download:

            bar = tqdm(total=total_size, unit='B', unit_scale=True) if show_progress else None

            target_file.parent.mkdir(exist_ok=True, parents=True)

            with open(target_file.as_posix(), 'wb') as f:
                ftp.retrbinary(f'RETR {parsed_url.path}',
                               partial(cls._ftp_writer, f, bar=bar),
                               blocksize=cls.BLOCK_SIZE)
            bar.close()

        return target_file

    @classmethod
    def _fetch_generic_file(cls, source_file: FileCache, target_file: Path, headers=None, auth=None,
                            show_progress=False, force_download=False):

        transport_params = {}
        url = furl(source_file.url)
        if headers:
            transport_params['headers'] = headers
        if auth:
            transport_params.update(auth)
        if source_file.url_params:
            url.add(source_file.url_params)

        with smart_open(url.url, 'rb', ignore_ext=True, transport_params=transport_params) as r:
            total_size = r.content_length
            if cls._requires_update(target_file, total_size, 7) or force_download:
                target_file.parent.mkdir(exist_ok=True, parents=True)
                bar = tqdm(total=total_size, unit='B', unit_scale=True) if show_progress else None
                with open(target_file.as_posix(), 'wb') as f:
                    # this is actually identical to shutil.copyfileobj(r.raw, f)
                    # but with tqdm injected to show progress
                    cls._generic_writer(r, f, block_size=cls.BLOCK_SIZE, bar=bar)

        return target_file

    @staticmethod
    def _ftp_writer(dest, block, bar=None):
        if bar:
            bar.update(len(block))
        dest.write(block)

    @staticmethod
    def _requires_update(target_file: Path, total_size: int, time_delta: int = None) -> bool:

        if target_file.exists():
            stat = target_file.stat()
            existing_size = stat.st_size
            # if either the sizes match up or the size can't be obtained but the file is recent
            return not ((existing_size == total_size)
                        or (total_size == -1 and
                            (datetime.now() - datetime.fromtimestamp(stat.st_mtime)).days < time_delta))
        else:
            return True

    @staticmethod
    def _generic_writer(source, target, block_size=16384, bar=None):

        while 1:
            buf = source.read(block_size)
            if not buf:
                break
            if bar:
                bar.update(len(buf))
            target.write(buf)
        if bar:
            bar.close()

    def get_file(self, source_file: FileCache, target_file: Path, show_progress=False, force_download=False):

        try:
            session = get_session()
            parsed_url = up.urlparse(source_file.url)
            if parsed_url.scheme == 'ftp':
                result = self._fetch_ftp_file(source_file.url, target_file,
                                              show_progress=show_progress,
                                              force_download=force_download)
            else:
                result = self._fetch_generic_file(source_file, target_file,
                                                  headers=self.headers,
                                                  auth=self.auth,
                                                  show_progress=show_progress,
                                                  force_download=force_download)

            self._update_file_cache(session, source_file, result)
            return result

        except Exception as ex:
            print(f'Could not download {source_file.url}: {ex}')
            return None

    @staticmethod
    def _update_file_cache(session, source_file, target_file: Path):

        source_file.path = str(target_file)
        source_file.hash = source_file.file_hash.hexdigest()
        source_file.last_download = datetime.now()
        source_file.size = target_file.stat().st_size
        session.add(source_file)
        session.commit()
