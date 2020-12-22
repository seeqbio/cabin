import os
from pathlib import Path

# TODO document

SGX_MYSQL_HOST = os.environ.get('SGX_MYSQL_HOST', 'localhost')
SGX_MYSQL_DB = 'sgx_biodb'
SGX_MYSQL_READER_PASSWORD = 'streamline'
SGX_MYSQL_WRITER_PASSWORD = 'streamline'

SGX_S3_ARCHIVE_BUCKET = os.environ.get('SGX_S3_ARCHIVE_BUCKET', 'sgx-archives')
SGX_S3_ARCHIVE_PREFIX = os.environ.get('SGX_S3_ARCHIVE_PREFIX', 'database/')

SGX_S3_CACHE_BUCKET = os.environ.get('SGX_S3_CACHE_BUCKET', 'sgx-cache')
SGX_S3_CACHE_PREFIX = os.environ.get('SGX_S3_CACHE_PREFIX', 'biodb/')

SGX_DOWNLOAD_DIR = os.environ.get('SGX_DOWNLOAD_DIR', 'downloads')

SGX_MYSQL_PROFILE = False


def _git_version(root):
    """Returns the current version string of this repo as per `git describe`."""
    import subprocess
    git_cmd = ['git', '-C', str(root), 'describe', '--tag', '--dirty']
    proc = subprocess.Popen(git_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate()
    if proc.returncode:
        raise RuntimeError(err.decode('utf-8') + '\nfailed to get git repo version: '
                           'are there any git tags?')
    version = out.decode('utf-8').strip()
    return version


SGX_ROOT_DIR = Path(__file__).parents[1].absolute()
SGX_GIT_VERSION = _git_version(SGX_ROOT_DIR)
SGX_SCHEMA_DIR = SGX_ROOT_DIR / 'schema'


# NOTE to be revised w ops, temporary solution: all biodb instances write to
# the same S3 storage.
SGX_S3_MIRROR_BUCKET = os.environ.get('SGX_S3_MIRROR_BUCKET', 'sgx-archives')
SGX_S3_MIRROR_PREFIX = os.environ.get('SGX_S3_MIRROR_PREFIX', 'biodb/mirrors').rstrip('/')
