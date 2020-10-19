import re
import boto3
import hashlib
from pathlib import Path
from botocore.exceptions import ClientError
from distutils.version import LooseVersion

from abc import ABC
from abc import abstractmethod

from biodb import log
from biodb import BiodbError
from biodb import ProgrammingError
from biodb import AbstractAttribute
from biodb import settings
from biodb.io import wget


class DatasetVersion:

    def __init__(self, source=None, checksum=None, git=None):
        self.source = source
        self.checksum = checksum
        self.git = git
        self.validate(self)

    def __str__(self):
        return self.serialize()

    def __eq__(self, other):
        if isinstance(other, DatasetVersion):
            attrs = ['source', 'checksum', 'git']
            return all(getattr(self, attr) == getattr(other, attr) for attr in attrs)
        return False

    def __hash__(self):
        return hash((self.source, self.checksum, self.git))

    def sub(self, upto='git'):
        components = ['source', 'checksum', 'git']
        values = [getattr(self, k) for k in components]
        upto_idx = components.index(upto) + 1
        return self.__class__(**dict(zip(components[:upto_idx], values[:upto_idx])))

    @classmethod
    def validate(cls, version):
        if version.checksum:
            if not version.source:
                raise BiodbError('source version is required if checksum is defined')

        if version.git:
            if not version.source:
                raise BiodbError('source version is required if git version is defined')
            if not version.checksum:
                raise BiodbError('checksum is required if git version is defined')

    @classmethod
    def parse(cls, string):
        token_names = ['source', 'checksum', 'git']
        token_values = string.split('::')
        token_values += [None] * (len(token_names) - len(token_values))
        version = cls(**dict(zip(token_names, token_values)))
        return version

    def serialize(self, upto='git'):
        if not self.source:
            raise ProgrammingError('Cannot serialize a DatasetVersion without source version')

        output = self.source
        if not self.checksum or upto == 'source':
            return output

        output += '::' + self.checksum
        if not self.git or upto == 'checksum':
            return output

        output += '::' + self.git
        return output


class BaseDataset(ABC):
    file_extension = AbstractAttribute()
    name = AbstractAttribute()

    def __init__(self, app, version, downloaded=None, archived=None, imported=None):
        self.version = version
        self.app = app

        # if any of the status attributes (downloaded, archived, imported) are
        # set to any value but None we set the corresponding instance attribute
        # which bypasses the live-checking of the respective value in
        # __getattr__ which is called by python as a last resort.
        if downloaded is not None:
            self.downloaded = bool(downloaded)
        if archived is not None:
            self.archived = bool(archived)
        if imported is not None:
            self.imported = bool(imported)

    # TODO document that any change to self.version should update status
    # attributes accordingly (or delete them) because it version affects paths
    def __getattr__(self, attr):
        # NOTE lazy-loads and sets attribute so that __getattr__ will not be
        # called twice with the same attribute (note: python calls __getattr__
        # as a last-resort).
        if attr == 'downloaded':
            self.downloaded = self._downloaded()
            return self.downloaded
        elif attr == 'archived':
            self.archived = self._archived()
            return self.archived
        elif attr == 'imported':
            self.imported = self._imported()
            return self.imported
        else:
            raise AttributeError(attr)

    @property
    def label(self):
        return '"{name}" version "{version}"'.format(name=self.name, version=self.version)

    # ====================
    # download and archive
    # ====================
    @property
    def basename(self):
        return '{name}::{version}'.format(
            name=self.name,
            version=self.version.serialize(upto='checksum'),
        )

    @property
    def filename(self):
        return '{basename}.{ext}'.format(
            basename=self.basename,
            ext=self.file_extension
        )

    @property
    def download_path(self):
        return Path(settings.SGX_DOWNLOAD_DIR) / self.filename

    @property
    def archive_bucket(self):
        return settings.SGX_S3_ARCHIVE_BUCKET

    @property
    def archive_key(self):
        return Path(settings.SGX_S3_ARCHIVE_PREFIX) / self.filename

    @property
    @abstractmethod
    def source_url(self):
        pass

    @classmethod
    def sha256(cls, path, buffer_size=int(1e6)):
        log('Calculating SHA256 of file %s' % path)
        sha = hashlib.sha256()
        with path.open('rb') as f:
            data = f.read(buffer_size)
            while data:
                sha.update(data)
                data = f.read(buffer_size)
        return sha.hexdigest()[:8]

    def _downloaded(self):
        return self.download_path.exists()

    def _archived(self):
        try:
            s3 = boto3.client('s3')
            s3.head_object(Bucket=self.archive_bucket, Key=str(self.archive_key))
            return True
        except ClientError:
            return False

    def download(self):
        self.download_path.parent.mkdir(parents=True, exist_ok=True)
        if self.version.checksum:
            if self.downloaded:
                log('Downloaded copy already exists!')
                return

            log('Downloading %s from archive.' % self.label)
            self.download_from_archive()
        else:
            self.download_from_source()

        assert self.version.checksum
        self.downloaded = True
        log('Downloaded %s to: %s' % (self.label, self.download_path))

    def download_from_archive(self):
        assert self.version.checksum

        dl_path = self.download_path
        key = str(self.archive_key)

        s3 = boto3.resource('s3')
        s3.Bucket(self.archive_bucket).download_file(key, str(dl_path))

        checksum = self.sha256(dl_path)
        if checksum != self.version.checksum:
            dl_path.unlink()
            raise BiodbError('Downloaded archive file has a different checksum (%s) than expected (%s)' % (checksum, self.version.checksum))

    def download_from_source(self):
        assert not self.version.checksum

        # since we don't have a self.version.checksum yet this path will
        # only contain the source version; we will move the downloaded file
        # after we have calculated its checksum
        dl_path = self.download_path

        log('downloading {label} from source at:\n\t{u}'.format(label=self.label, u=self.source_url))
        retcode = wget(self.source_url, dl_path)
        if retcode:
            dl_path.unlink()
            raise BiodbError('Download failed!')

        self.version.checksum = self.sha256(dl_path)

        # now that self.version.checksum is set self.download_path produces the
        # final path for the downloaded file which includes the checksum.
        dl_path.rename(self.download_path)

    def archive(self):
        if not self.version.checksum:
            raise ProgrammingError('Refusing to archive when the dataset has no checksum: ' + self.label)

        log('Archiving ' + self.label)
        if self.archived:
            log('Archived copy already exists!')
            return

        self.archive_real()
        self.archived = True
        log('Successfully archived ' + self.label)

    def archive_real(self):
        path = self.download_path
        key = str(self.archive_key)
        s3 = boto3.resource('s3')
        try:
            s3.Bucket(self.archive_bucket).upload_file(str(path), key)
        except ClientError:
            raise BiodbError('Upload failed!')

    # ======
    # import
    # ======
    def _imported(self):
        with self.app.mysql.cursor('reader') as cursor:
            cursor.execute('SHOW TABLES LIKE "{n}";'.format(n=self.table_name))
            return bool(cursor.fetchall())

    @property
    def schema_path(self):
        return self.app.schema_dir / '{name}.sql'.format(name=self.name)

    @property
    def sql_create(self):
        with self.schema_path.open() as f:
            return f.read().format(table=self.table_name)

    @property
    def table_name(self):
        name = '{name}::{version}'.format(name=self.name, version=self.version.serialize(upto='git'))
        return name

    @property
    def sql_drop(self):
        return 'DROP TABLE IF EXISTS `{table}`;'.format(table=self.table_name)

    def import_(self):
        self.download()
        self.version.git = self.app.git_version
        log('Importing ' + self.label)
        if self.imported:
            log('Imported copy of %s already exists!' % self.label)
            return
        self.import_real()
        self.imported = True
        log('Successfully imported ' + self.label)

    @abstractmethod
    def import_real(self):
        pass

    def drop(self):
        log('dropping ' + self.label)
        with self.app.mysql.transaction() as cursor:
            cursor.execute(self.sql_drop)

    # ======
    # search
    # ======
    @classmethod
    def deserialize_version(cls, fname, include_extension=True):
        pattern = '{n}::(?P<version>.*)'.format(n=cls.name)
        if include_extension:
            pattern += '.' + cls.file_extension
        match = re.match(pattern, fname)
        if not match:
            return
        return DatasetVersion.parse(match.group('version'))

    @classmethod
    def downloaded_versions(cls, app):
        download_dir = Path(settings.SGX_DOWNLOAD_DIR)
        pattern = '{n}*.{e}'.format(n=cls.name, e=cls.file_extension)
        for path in download_dir.glob(pattern):
            version = cls.deserialize_version(path.name)
            if version:
                yield version

    @classmethod
    def archived_versions(cls, app):
        s3 = boto3.client('s3')
        res = s3.list_objects(Bucket=settings.SGX_S3_ARCHIVE_BUCKET,
                              Prefix=settings.SGX_S3_ARCHIVE_PREFIX)
        for s3_obj in res.get('Contents', []):
            version = cls.deserialize_version(Path(s3_obj['Key']).name)
            if version:
                yield version

    @classmethod
    def imported_versions(cls, app):
        with app.mysql.cursor('reader') as cursor:
            cursor.execute('SHOW TABLES LIKE "{n}::%";'.format(n=cls.name))
            for row in cursor:
                table_name = row[0]
                version = cls.deserialize_version(table_name, include_extension=False)
                if version:
                    yield version

    @classmethod
    def search(cls, app):
        """Collects all downloaded, archived, and imported copies of this
        dataset and coalesces them based on version."""
        i_versions = {version: True for version in cls.imported_versions(app)}
        a_versions = {version: True for version in cls.archived_versions(app)}
        d_versions = {version: True for version in cls.downloaded_versions(app)}

        # go through each of the 3 dictionaries (order of operations matter);
        # when visiting each dictionary pop identical versions from other
        # dictionaries. Keep a working copy so we don't lose information while
        # processing copies in the same bunch (e.g. we don't want to forget that
        # an archived copy exists as go through multiple imported versions with
        # the same source version and checksum.
        working_a_versions = a_versions.copy()
        working_d_versions = d_versions.copy()
        for version in i_versions:
            subversion = version.sub(upto='checksum')
            a_versions.pop(subversion, None)
            d_versions.pop(subversion, None)
            yield cls(app=app,
                      version=version,
                      downloaded=working_d_versions.get(subversion, False),
                      archived=working_a_versions.get(subversion, False),
                      imported=True)

        # remaining archived versions are guaranteed to not be imported
        working_d_versions = d_versions.copy()
        for version in a_versions:
            subversion = version.sub(upto='checksum')
            d_versions.pop(subversion, None)
            yield cls(app=app,
                      version=version,
                      downloaded=working_d_versions.get(version, False),
                      archived=True,
                      imported=False)

        # remaining downloaded versions are guaranteed to not be archived or imported
        for version in d_versions:
            yield cls(app=app,
                      version=version,
                      downloaded=True,
                      archived=False,
                      imported=False)

    @classmethod
    def search_sorted(cls, app):
        def sort_key(dataset):
            # LooseVersion gets confused when there are mixed integer and
            # string components to version strings, cf. https://bugs.python.org/issue14894
            # leading to `TypeError: unorderable types: int() < str()`
            # make our own tuple from LooseVersion parsed version token list
            return tuple(str(x) for x in LooseVersion(dataset.version.source).version)

        return sorted(cls.search(app), key=sort_key)
