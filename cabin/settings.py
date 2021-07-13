import os
from pathlib import Path

# see ops.SGX_INSTANCE_ID
SGX_INSTANCE_ID = os.environ.get('SGX_INSTANCE_ID')

SGX_MYSQL_HOST = 'mysql'
SGX_MYSQL_DB = 'sgx_biodb'
SGX_MYSQL_READER_PASSWORD = 'streamline'
SGX_MYSQL_WRITER_PASSWORD = os.environ.get('SGX_MYSQL_WRITER_PASSWORD')
SGX_MYSQL_CNX_TIMEOUT = int(os.environ.get('SGX_MYSQL_CNX_TIMEOUT', 30))

SGX_DOWNLOAD_DIR = 'downloads'
SGX_MYSQL_PROFILE = False

SGX_ROOT_DIR = Path(__file__).parents[1].absolute()

SGX_S3_MIRROR_BUCKET = os.environ.get('SGX_S3_MIRROR_BUCKET', 'sgx-archives')
SGX_S3_MIRROR_PREFIX = os.environ.get('SGX_S3_MIRROR_PREFIX', 'biodb/mirrors').rstrip('/')

SGX_NON_INTERACTIVE = 'CI_PIPELINE_ID' in os.environ
