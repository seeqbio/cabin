import os

# see ops.CABIN_INSTANCE_ID
CABIN_INSTANCE_ID = '1'#os.environ.get('CABIN_INSTANCE_ID')

CABIN_MYSQL_HOST = os.environ.get('CABIN_DB_HOST', '127.0.0.1')
CABIN_MYSQL_DB = os.environ.get('CABIN_DB_NAME', 'cabin')
CABIN_MYSQL_READER_USER = 'cabin'
CABIN_MYSQL_WRITER_USER = 'cabin'
CABIN_MYSQL_READER_PASSWORD = 'cabin'
CABIN_MYSQL_WRITER_PASSWORD = 'cabin'
CABIN_MYSQL_CNX_TIMEOUT = int(os.environ.get('CABIN_MYSQL_CNX_TIMEOUT', 30))

CABIN_DOWNLOAD_DIR = os.environ.get('CABIN_DOWNLOADS', '/tmp/cabin/downloads')

CABIN_S3_MIRROR_BUCKET = os.environ.get('CABIN_S3_MIRROR_BUCKET', 'cabin-archives')
CABIN_S3_MIRROR_PREFIX = os.environ.get('CABIN_S3_MIRROR_PREFIX', 'cabin/mirrors').rstrip('/')

CABIN_NON_INTERACTIVE = 'CI_PIPELINE_ID' in os.environ
