import urllib.parse as up
import zipfile
import shutil
import gzip
import tarfile

from abc import abstractmethod
from datetime import datetime
from dataclasses import dataclass
from ftplib import FTP
from functools import partial
from pathlib import Path
from typing import List, Union, Optional, Set

from furl import furl
from smart_open import open as smart_open
from tqdm import tqdm

from ketl.db.models import API, CachedFile, ExpectedFile
from ketl.db.settings import get_session

# from src.network.apis import BaseAPI


@dataclass
class SourceTargetPair:

    source: Union[str, CachedFile]
    target: Union[str, Path]


class BaseExtractor:

    @abstractmethod
    def extract(self) -> List[Path]:

        raise NotImplementedError('extract not implemented in the base class')


class DefaultExtractor(BaseExtractor):

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
                for source_file in source.source_files]  # type: CachedFile

    def extract(self) -> List[Path]:

        results = list(filter(None, [self.get_file(st_pair.source, st_pair.target, show_progress=True)
                                     for st_pair in self.source_target_list]))

        expected_files = []
        for source_file in results:
            source_file.uncompress()  # safe to call on non-archives since nothing will happen
            expected_files.extend(Path(file) for file in source_file.expected_files)
        return expected_files

    @classmethod
    def _fetch_ftp_file(cls, source_file: CachedFile, target_file: Path,
                        show_progress=False, force_download=False) -> bool:

        parsed_url = up.urlparse(source_file.url)
        ftp = FTP(parsed_url.hostname)
        ftp.login()
        total_size = ftp.size(parsed_url.path)
        updated = False

        if cls._requires_update(target_file, total_size, 7) or force_download:

            bar = tqdm(total=total_size, unit='B', unit_scale=True) if show_progress else None

            target_file.parent.mkdir(exist_ok=True, parents=True)

            with open(target_file.as_posix(), 'wb') as f:
                ftp.retrbinary(f'RETR {parsed_url.path}',
                               partial(cls._ftp_writer, f, bar=bar),
                               blocksize=cls.BLOCK_SIZE)
            bar.close()
            updated = True

        return updated

    @classmethod
    def _fetch_generic_file(cls, source_file: CachedFile, target_file: Path, headers=None, auth=None,
                            show_progress=False, force_download=False) -> bool:

        transport_params = {}
        url = furl(source_file.url)
        if headers:
            transport_params['headers'] = headers
        if auth:
            transport_params.update(auth)
        if source_file.url_params:
            url.add(source_file.url_params)

        updated = False
        with smart_open(url.url, 'rb', ignore_ext=True, transport_params=transport_params) as r:
            total_size = getattr(r, 'content_length', -1)
            if cls._requires_update(target_file, total_size, 7) or force_download:
                source_file.fresh_data = True
                target_file.parent.mkdir(exist_ok=True, parents=True)
                bar = tqdm(total=total_size, unit='B', unit_scale=True) if show_progress else None
                with open(target_file.as_posix(), 'wb') as f:
                    # this is actually identical to shutil.copyfileobj(r.raw, f)
                    # but with tqdm injected to show progress
                    cls._generic_writer(r, f, block_size=cls.BLOCK_SIZE, bar=bar)
                updated = True

        return updated

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

    def get_file(self, source_file: CachedFile, target_file: Path,
                 show_progress=False, force_download=False) -> Optional[CachedFile]:

        try:
            parsed_url = up.urlparse(source_file.url)
            if parsed_url.scheme == 'ftp':
                result = self._fetch_ftp_file(source_file, target_file,
                                              show_progress=show_progress,
                                              force_download=force_download)
            else:
                result = self._fetch_generic_file(source_file, target_file,
                                                  headers=self.headers,
                                                  auth=self.auth,
                                                  show_progress=show_progress,
                                                  force_download=force_download)

            if result:
                self._update_file_cache(source_file, target_file)
                return source_file
            else:
                return None

        except Exception as ex:
            print(f'Could not download {source_file.url}: {ex}')
            return None

    @staticmethod
    def _update_file_cache(source_file, target_file: Path):

        session = get_session()
        source_file.path = str(target_file)
        source_file.hash = source_file.file_hash.hexdigest()
        source_file.last_download = datetime.now()
        source_file.size = target_file.stat().st_size
        session.add(source_file)
        session.commit()
