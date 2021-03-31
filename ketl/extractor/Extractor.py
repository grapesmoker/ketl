import urllib.parse as up
import asyncio

from abc import abstractmethod
from datetime import datetime, timedelta
from dataclasses import dataclass
from ftplib import FTP
from functools import partial
from pathlib import Path
from typing import List, Union, Optional, Iterator
from urllib.parse import quote
from multiprocessing.pool import Pool

from furl import furl
from smart_open import open as smart_open
from tqdm import tqdm
from sqlalchemy.orm import defer, Query
from more_itertools import chunked

from ketl.db.models import API, CachedFile, ExpectedFile, Source
from ketl.db.settings import get_session
from ketl.utils.file_utils import file_hash


class BaseExtractor:

    @abstractmethod
    def extract(self) -> List[Path]:

        raise NotImplementedError('extract not implemented in the base class')


class DefaultExtractor(BaseExtractor):
    """
    The default extractor can fetch files from an FTP server or any location that
    is openable via smart_open. It is up to the user to provide any credentials
    that are required to access the desired resources.
    """
    BLOCK_SIZE = 16384

    def __init__(self, api_config: Union[API, int, str], skip_exiting_files: bool = False,
                 show_progress: bool = False, concurrency: str = 'sync',
                 on_disk_check='full'):

        if type(api_config) is int:
            self.api = get_session().query(API).filter(API.id == api_config).one()
        elif type(api_config) is str:
            self.api = get_session().query(API).filter(API.name == api_config).one()
        elif isinstance(api_config, API):
            self.api = api_config
        self.headers = {}
        self.auth = None
        self.auth_token = None
        self.skip_existing_files = skip_exiting_files
        self.show_progress = show_progress
        self.concurrency = concurrency
        self.on_disk_check = on_disk_check

        if self.api.creds:
            details = self.api.creds.creds_details
            cookie = details.get('cookie', None)
            if cookie:
                self.headers['Cookie'] = cookie['name'] + '=' + cookie['value']
            self.auth = details.get('auth', None)
            self.auth_token = details.get('auth_token', None)
            if self.auth_token:
                self.headers[self.auth_token['header']] = self.auth_token['token']

    def extract(self) -> List[Path]:

        session = get_session()

        # depending on whether we are skipping files known to be on disk
        # we produce an iterable that is either a list of queries that will
        # give us the files that are missing, or a chunked version of a query
        if self.skip_existing_files:
            kwargs = {'missing': True, 'use_hash': self.on_disk_check == 'hash'}
            data_iterator: Query = self.api.cached_files_on_disk(**kwargs)
        else:
            data_iterator: Query = self.api.cached_files.options(defer(CachedFile.meta))

        for batch in chunked(data_iterator, 10000):  # type: List[CachedFile]

            results = []

            if self.concurrency == 'sync':
                results = list(
                    filter(None, [
                        self.get_file(
                            cached_file.id, cached_file.full_url, cached_file.full_path,
                            cached_file.refresh_interval, cached_file.url_params,
                            show_progress=self.show_progress
                        ) for cached_file in batch])
                )
            elif self.concurrency == 'async':
                raise NotImplementedError('Async downloads not yet implemented.')
            elif self.concurrency == 'multiprocess':
                get_file_args = [(
                    cached_file.id, cached_file.full_url, cached_file.full_path,
                    cached_file.refresh_interval, cached_file.url_params,
                    self.show_progress
                ) for cached_file in batch]

                if get_file_args:
                    with Pool() as pool:
                        futures = pool.starmap_async(self.get_file, get_file_args)
                        results = futures.get()
                        if results:
                            results = list(filter(None, results))
                    pool.join()

            session.bulk_update_mappings(CachedFile, results)
            session.commit()

        new_expected_files: List[dict] = []
        updated_expected_files: List[dict] = []

        q: Query = session.query(
            ExpectedFile.path, ExpectedFile.cached_file_id, ExpectedFile.id
        ).join(
            CachedFile, ExpectedFile.cached_file_id == CachedFile.id
        ).join(
            Source, CachedFile.source_id == Source.id
        ).filter(
            Source.api_config_id == self.api.id
        )

        current_files = {(ef[0], ef[1]): ef[2] for ef in q.yield_per(10000)}

        for source_file in self.api.cached_files:
            ef = source_file.preprocess()
            if ef:
                key = (ef['path'], ef['cached_file_id'])
                if key not in current_files:
                    new_expected_files.append(ef)
                else:
                    updated_expected_files.append({'id': current_files[key], **ef})

        session.bulk_insert_mappings(ExpectedFile, new_expected_files)
        session.bulk_update_mappings(ExpectedFile, updated_expected_files)
        session.commit()

        return [Path(ef.path) for ef in self.api.expected_files]

    @classmethod
    def _fetch_ftp_file(cls, source_url: str, target_file: Path, refresh_interval: timedelta,
                        show_progress=False, force_download=False) -> bool:

        parsed_url = up.urlparse(source_url)
        ftp = FTP(parsed_url.hostname)
        ftp.login()
        total_size = ftp.size(parsed_url.path)
        updated = False

        if cls._requires_update(target_file, total_size, refresh_interval) or force_download:

            bar = tqdm(total=total_size, unit='B', unit_scale=True) if show_progress else None

            target_file.parent.mkdir(exist_ok=True, parents=True)

            with open(target_file.as_posix(), 'wb') as f:
                ftp.retrbinary(f'RETR {parsed_url.path}',
                               partial(cls._ftp_writer, f, bar=bar),
                               blocksize=cls.BLOCK_SIZE)
            if bar:
                bar.close()
            updated = True

        return updated

    @classmethod
    def _fetch_generic_file(cls, source_url: str, target_file: Path, refresh_interval: timedelta,
                            url_params=None, headers=None, auth=None,
                            show_progress=False, force_download=False) -> bool:

        transport_params = {}
        url = furl(source_url)
        if headers:
            transport_params['headers'] = headers
        if auth:
            transport_params.update(auth)
        if url_params:
            url.add(url_params)

        # tragic hack that is necessitated by s3's failure to properly conform to http spec
        # c.f. https://forums.aws.amazon.com/thread.jspa?threadID=55746

        url_to_fetch = url.url
        if url.scheme in {'s3', 's3a'} or url.host == 's3.amazonaws.com':
            url_to_fetch = f'{url.scheme}://{url.host}/{quote(str(url.path))}'
            if url.fragmentstr != '':
                url_to_fetch += quote(f'#{url.fragmentstr}', safe='%')
            if url.querystr != '':
                url_to_fetch += f'&{url.querystr}'

        updated = False
        with smart_open(url_to_fetch, 'rb', ignore_ext=True, transport_params=transport_params) as r:
            total_size = getattr(r, 'content_length', -1)
            if cls._requires_update(target_file, total_size, refresh_interval) or force_download:
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
    def _requires_update(target_file: Path, total_size: int, time_delta: timedelta = None) -> bool:

        if target_file.exists():
            stat = target_file.stat()
            existing_size = stat.st_size
            # if either the sizes match up or the size can't be obtained but the file is recent
            return not ((existing_size == total_size)
                        or (total_size == -1 and
                            (datetime.now() - datetime.fromtimestamp(stat.st_mtime)) < time_delta))
        else:
            return True

    @staticmethod
    def _generic_writer(source, target, block_size=16384, bar=None):

        while 1:
            buf = source.read(block_size)
            if bar:
                bar.update(len(buf))
            if not buf:
                break
            target.write(buf)
        if bar:
            bar.close()

    def get_file(self, cached_file_id: int, source_url: str, target_file: Path, refresh_interval: timedelta,
                 url_params=None, show_progress=False, force_download=False) -> Optional[dict]:

        try:
            parsed_url = up.urlparse(source_url)
            if parsed_url.scheme == 'ftp':
                result = self._fetch_ftp_file(source_url,
                                              show_progress=show_progress,
                                              force_download=force_download)
            else:
                result = self._fetch_generic_file(source_url,
                                                  headers=self.headers,
                                                  auth=self.auth,
                                                  show_progress=show_progress,
                                                  force_download=force_download)

            if result:
                return {
                    'id': cached_file_id,
                    'hash': file_hash(target_file).hexdigest(),
                    'last_download': datetime.now(),
                    'size': target_file.stat().st_size
                }
            else:
                return None

        except Exception as ex:
            print(f'Could not download {source_url}: {ex}')
            return None

    @staticmethod
    def _update_file_cache(source_file: CachedFile, target_file: Path):

        session = get_session()
        source_file.hash = file_hash(target_file).hexdigest()
        source_file.last_download = datetime.now()
        source_file.size = target_file.stat().st_size
        session.add(source_file)
        session.commit()
