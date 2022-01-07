import os

# see ops.SGX_INSTANCE_ID
SGX_INSTANCE_ID = '1'#os.environ.get('SGX_INSTANCE_ID')

SGX_MYSQL_HOST = 'localhost' # 'mysql'
SGX_MYSQL_DB = 'cabin'
SGX_MYSQL_READER_USER = 'reader'
SGX_MYSQL_WRITER_USER = 'writer'
SGX_MYSQL_READER_PASSWORD = 'KAZ'
SGX_MYSQL_WRITER_PASSWORD = 'KAZ' # os.environ.get('SGX_MYSQL_WRITER_PASSWORD')
SGX_MYSQL_CNX_TIMEOUT = int(os.environ.get('SGX_MYSQL_CNX_TIMEOUT', 30))

SGX_DOWNLOAD_DIR = '/sgx/cabin/downloads'

SGX_S3_MIRROR_BUCKET = os.environ.get('SGX_S3_MIRROR_BUCKET', 'sgx-archives')
SGX_S3_MIRROR_PREFIX = os.environ.get('SGX_S3_MIRROR_PREFIX', 'cabin/mirrors').rstrip('/')

SGX_NON_INTERACTIVE = 'CI_PIPELINE_ID' in os.environ
