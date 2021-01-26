import os
from pathlib import Path

# TODO document

SGX_MYSQL_HOST = 'mysql'
SGX_MYSQL_DB = 'sgx_biodb'
SGX_MYSQL_READER_PASSWORD = 'streamline'
SGX_MYSQL_WRITER_PASSWORD = 'streamline'

SGX_DOWNLOAD_DIR = 'downloads'
SGX_MYSQL_PROFILE = False

SGX_ROOT_DIR = Path(__file__).parents[1].absolute()
SGX_SCHEMA_DIR = SGX_ROOT_DIR / 'schema'

# NOTE to be revised w ops, temporary solution: all biodb instances write to
# the same S3 storage.
SGX_S3_MIRROR_BUCKET = os.environ.get('SGX_S3_MIRROR_BUCKET', 'sgx-archives')
SGX_S3_MIRROR_PREFIX = os.environ.get('SGX_S3_MIRROR_PREFIX', 'biodb/mirrors').rstrip('/')
