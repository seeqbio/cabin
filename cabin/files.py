import os
import boto3
import botocore.exceptions

from pathlib import Path
from abc import abstractmethod

from . import logger, settings, AbstractAttribute
from .io import wget
from .core import Dataset


class ExternalFile(Dataset):
    """The most common (and happy) scenario for external resources, e.g. FTP
    URL for ClinVar VCF. It assumes that if a URL includes a token that is
    advertised as a version (e.g. dbNSFP "3.5c" or ClinVar "2020-04"), we will
    always get the same data when we hit the same URL.

    Some Datasets are not properly versionable, see S3MirrorFile.
    """
    depends = {}

    @property
    @abstractmethod
    def url(self):
        pass

    def exists(self):
        return True

    def produce(self):
        # we cannot produce external files, they are just expected to be where
        # they are supposed to be. Or they aren't but that's a problem for
        # exists() and produce_recursive().
        assert self.exists(), \
            'Cannot produce external file at version %s for %s!' % (self.version, self.name)


class FTPTimestampedFile(ExternalFile):
    ftp_server = AbstractAttribute
    ftp_path = AbstractAttribute

    @property
    def url(self):
        assert self.exists()
        return 'ftp://{server}/{path}'.format(server=self.ftp_server, path=self.ftp_path)

    def exists(self):
        assert self.current_ftp_version == self.version, \
            'Dataset %s is not available, available version: "%s"' % (self.name, self.current_ftp_version)
        return True

    @property
    def current_ftp_version(self):
        from biodb.io import ftp_modify_time
        if not hasattr(self, '_current_version'):
            timestamp = ftp_modify_time(self.ftp_server, self.ftp_path)
            self._current_version = str(timestamp.date())
        return self._current_version


class LocalFile(Dataset):
    extension = None

    @property
    def path(self):
        return '{downloads}/{name}.{ext}'.format(
            downloads=settings.SGX_DOWNLOAD_DIR,
            name=self.name,
            ext=self.extension
        )

    def exists(self):
        return Path(self.path).exists()

    def produce(self):
        Path(settings.SGX_DOWNLOAD_DIR).mkdir(exist_ok=True, parents=True)
        try:
            wget(self.input.url, self.path)
        except: # any exception, even KeyboardInterrupt
            logger.info('Removing partially downloaded file %s' % self.path)
            os.unlink(self.path)
            raise


class S3MirrorFile(Dataset):
    """S3 Mirrors are a mechanism for handling external data sources for which
    we don't like the original URL, for whatever reason. We modify the
    dependency chain like this:

        bad URL -> S3 mirror -> local download

    An S3 mirror's job is to download the original (bad) URL once and serve as
    a good, reliable source for its downstream. Example use cases:

        * dbNSFP's official URL is very slow
        * a lot of NCBI datasets (e.g. gene2refseq) are not properly
          versionable: URLs have no version in them and we use the FTP timestamp
          as the version. This means that if I download 2020-09-01 today, I
          will not be able to download it tomorrow (only 2020-09-02 is
          available tomorrow).
    """
    extension = AbstractAttribute()

    @property
    def s3_key(self):
        return '{prefix}/{name}.{ext}'.format(
            prefix=settings.SGX_S3_MIRROR_PREFIX,
            name=self.name,
            ext=self.extension
        )

    def local_download_path(self):
        # convenience magic: make the local path the same as downstream so we
        # don't have to re-download this again.
        rdepends = self.rdepends()
        if len(rdepends) == 1:
            return rdepends[0]().path

        return '/tmp/{name}.{ext}'.format(name=self.name, ext=self.extension)

    def produce(self):
        # download from external URL to local disk
        local_path = self.local_download_path()
        wget(self.input.url, str(local_path))

        # upload from local disk to S3
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(settings.SGX_S3_MIRROR_BUCKET)
            bucket.upload_file(str(local_path), self.s3_key)
        except botocore.exceptions.ClientError:
            raise BiodbError('Upload failed!')

    def exists(self):
        try:
            s3 = boto3.client('s3')
            s3.head_object(Bucket=settings.SGX_S3_MIRROR_BUCKET, Key=str(self.s3_key))
            return True
        except botocore.exceptions.ClientError:
            return False


class S3MirroredLocalFile(LocalFile):
    """Local copy of an S3MirrorFile."""

    def produce(self):
        # special case: the file might already exist, thanks to the produce()
        # of the S3MirrorFile dependency.
        if not self.exists():
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(settings.SGX_S3_MIRROR_BUCKET)
            bucket.download_file(self.input.s3_key, str(self.path))
