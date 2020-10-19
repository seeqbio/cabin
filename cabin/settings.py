import os

# TODO document

SGX_MYSQL_HOST = os.environ.get('SGX_MYSQL_HOST', 'localhost')
SGX_MYSQL_DB = 'sgx_biodb'
SGX_MYSQL_READER_PASSWORD = 'streamline'
SGX_MYSQL_WRITER_PASSWORD = 'streamline'

SGX_S3_ARCHIVE_BUCKET = os.environ.get('SGX_S3_ARCHIVE_BUCKET', 'sgx-archives')
SGX_S3_ARCHIVE_PREFIX = os.environ.get('SGX_S3_ARCHIVE_PREFIX', 'database/')

SGX_S3_CACHE_BUCKET = os.environ.get('SGX_S3_CACHE_BUCKET', 'sgx-cache')
SGX_S3_CACHE_PREFIX = os.environ.get('SGX_S3_CACHE_PREFIX', 'biodb/')

SGX_DOWNLOAD_DIR = 'download/'
