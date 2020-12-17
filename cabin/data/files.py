from pathlib import Path
from abc import abstractmethod

from .core import Dataset
from biodb.io import wget

# mock, local files in mock_storage/ mock all sorts of resources in this prototype
# (files, s3, docker images, EBS volumes, etc.)
def touch(path):
    path = Path(path)
    path.parent.mkdir(exist_ok=True, parents=True)
    path.touch()


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
    ftp_server = None
    ftp_path = None

    # mock, should look up FTP server instead
    actually_available_ftp_version = None

    @classmethod
    def validate_class(cls):
        super().validate_class()
        cls.assert_class_attributes(str, 'ftp_server', 'ftp_path')

    @property
    def url(self):
        return 'ftp://{server}/{path}'.format(server=self.ftp_server, path=self.ftp_path)

    def exists(self):
        # mock
        return self.version == self.actually_available_ftp_version


class LocalFile(Dataset):
    extension = None

    @classmethod
    def validate_class(cls):
        super().validate_class()
        cls.assert_class_attributes(str, 'extension')

    @property
    def path(self):
        return 'downloads/{name}.{ext}'.format(
            name=self.name,
            ext=self.extension
        )

    def exists(self):
        return Path(self.path).exists()

    def produce(self):
        wget(self.input.url, self.path)


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
    extension = None

    @classmethod
    def validate_class(cls):
        super().validate_class()
        cls.assert_class_attributes(str, 'extension')

    def drop(self):
        # TODO: rm file or remove drop() from superclass, only for ImportedTable?
        pass

    @property
    def s3_path(self):
        # include source version for intelligibility
        return 's3://sgx-cache/database/{name}.{ext}'.format(
            name=self.name,
            ext=self.extension
        )

    def produce(self):
        # mock
        print('$ wget {src} -O /dev/stdout | aws s3 cp - {dst}\n'.format(
            src=self.input.url,
            dst=self.s3_path
        ))

        touch(self.s3_path.replace('s3://', 'mock_storage/s3/'))

    def exists(self):
        return Path(self.s3_path.replace('s3://', 'mock_storage/s3/')).exists()


class S3MirroredLocalFile(LocalFile):
    """Local copy of an S3MirrorFile."""

    def produce(self):
        # mock
        print('$ aws s3 cp {src} {dst}\n'.format(
            src=self.input.s3_path,
            dst=self.path
        ))
        # mock download
        touch(self.path)
