import gzip
import tarfile
import zipfile
import shutil

from typing import Set
from hashlib import sha1
from pathlib import Path


def file_hash(path: Path, block_size=65536):
    s = sha1()

    if path.exists():
        with open(path, 'rb') as f:
            stop = False
            while not stop:
                data = f.read(block_size)
                if len(data) > 0:
                    s.update(data)
                else:
                    stop = True

    return s


def uncompress(file: Path, dest: Path) -> Set[Path]:

    if tarfile.is_tarfile(file):
        tf = tarfile.open('')

    if file.name.endswith('.zip'):
        zipped_target = zipfile.ZipFile(file)
        zipped_target.extractall(path=str(dest))
    elif file.name.endswith('.gz'):
        result_file = dest / file.stem
        with open(result_file, 'wb') as f_out:
            with gzip.open(file, 'r') as f_in:
                shutil.copyfileobj(f_in, f_out)
        if result_file.name.endswith('.tar'):
            tar_file = tarfile.TarFile(result_file)
            tar_file.extractall(path=dest)
    elif file.name.endswith('.tar'):
        tar_file = tarfile.TarFile(file)
        tar_file.extractall(path=dest)

    result_files = {extracted_file for extracted_file in dest.glob('**')
                    if extracted_file != file}
    return result_files
