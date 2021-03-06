import os

CABIN_MYSQL_HOST = os.environ.get('CABIN_DB_HOST', '127.0.0.1')
CABIN_MYSQL_DB = os.environ.get('CABIN_DB_NAME', 'cabin')
CABIN_MYSQL_USER =  os.environ.get('CABIN_MYSQL_USER', 'cabin')
CABIN_MYSQL_PASSWORD =  os.environ.get('CABIN_MYSQL_PASSWORD', 'cabin')
CABIN_MYSQL_CNX_TIMEOUT = int(os.environ.get('CABIN_MYSQL_CNX_TIMEOUT', 30))
CABIN_SYSTEM_TABLE = os.environ.get('CABIN_SYSTEM_TABLE', 'cabin_system')

CABIN_DOWNLOAD_DIR = os.environ.get('CABIN_DOWNLOADS', '/tmp/cabin/downloads')

CABIN_S3_MIRROR_BUCKET = os.environ.get('CABIN_S3_MIRROR_BUCKET', 'cabin-archives')
CABIN_S3_MIRROR_PREFIX = os.environ.get('CABIN_S3_MIRROR_PREFIX', 'cabin/mirrors').rstrip('/')

CABIN_NON_INTERACTIVE = 'CI_PIPELINE_ID' in os.environ
