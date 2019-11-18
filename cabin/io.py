import gzip
import allel
import subprocess
import numpy as np
from lxml import etree
from Bio import SeqIO
from ftplib import FTP
from dateutil import parser
from pathlib import Path

from biodb import log
from biodb import BiodbError


def read_xsv(path, delimiter='\t', columns=None, header_leading_hash=True, gzipped=False):
    path = str(path)
    f = gzip.open(path, 'rt') if gzipped else open(path, 'r')

    log('reading records from "{p}"'.format(p=f.name))

    if columns is None:
        header = f.readline().strip()
        if header_leading_hash:
            if header[0] != '#':
                raise BiodbError('Expected first line to start with #')
            header = header[1:]
        columns = header.split(delimiter)

    for line in f:
        values = line.strip().split(delimiter)
        yield dict(zip(columns, values))

    f.close()


def read_vcf(path, numbers, fields):
    path = str(path)
    log('reading VCF records from "{p}"'.format(p=path))
    _, _, _, chunk_iter = allel.iter_vcf_chunks(path, numbers=numbers, fields=fields, chunk_length=100)
    for chunk in chunk_iter:
        variants = chunk[0]
        # all variants[X] values are ndarrays with the same shape
        for row in zip(*(variants['variants/' + field] for field in fields)):
            row = [int(x) if isinstance(x, np.int32) else x for x in row]
            yield dict(zip(fields, row))


# A callback to free the memory used by elements retrieved by etree.iterparse
# https://stackoverflow.com/a/12161078
# https://www.ibm.com/developerworks/xml/library/x-hiperfparse/
def xml_element_clear_memory(elem):
    elem.clear()
    # Also eliminate now-empty references from the root node to elem,
    # otherwise these dangling elements will swallow a lot of memory!
    for ancestor in elem.xpath('ancestor-or-self::*'):
        while ancestor.getprevious() is not None:
            del ancestor.getparent()[0]


def xml_element_to_string(elem):
    return etree.tostring(elem, pretty_print=True, encoding='unicode')


def read_xml(source, tag):
    """source is either path to a plain text XML file or a file object that
    reads in bytes."""
    # use an incremental parser instead of loading the entire DOM in memory
    # NOTE users must clear the memory used by subelements retrieved via xpath
    # or the like after use.
    for _, elem in etree.iterparse(source, tag=tag):
        yield elem


def read_fasta(path, gzipped=False):
    f = gzip.open(path, 'rt') if gzipped else open(path, 'r')
    for record in SeqIO.parse(f, 'fasta'):
        yield record.id, str(record.seq)

    f.close()


def wget(source, destination):
    cmd = ['wget', '-q', '--show-progress', str(source), '-O', str(destination)]
    proc = subprocess.Popen(cmd)
    proc.communicate()
    return proc.returncode


def ftp_modify_time(ftp_server, ftp_path):
    """Returns a datetime object containing the modification time of a given
    FTP path."""

    ftp = FTP(ftp_server)
    ftp.login()
    # MDTM command gives the file modification time
    # https://tools.ietf.org/html/rfc3659#section-3
    resp_code, timestamp = ftp.voidcmd('MDTM ' + ftp_path).split()
    if resp_code == '213': # success
        return parser.parse(timestamp)
    else:
        ftp_url = 'ftp://{s}{p}'.format(s=ftp_server, p=ftp_path)
        raise BiodbError('Failed to get FTP file modification time for: ' + ftp_url)


def gunzip(src, dst):
    command = 'gunzip -c {src} > {dst}'.format(src=src, dst=dst)
    proc = subprocess.Popen(command, shell=True)
    proc.communicate()
    if proc.returncode:
        raise BiodbError('Failed to unzip: ' + str(src))


def unzip(zipname, extract_dir=None):
    log('Unzipping: ' + str(zipname))
    zipname = Path(zipname)
    if extract_dir is None:
        assert Path(zipname).suffix == '.zip', 'expected zip file name to end with ".zip"'
        extract_dir = zipname.parent / zipname.stem
    else:
        extract_dir = Path(extract_dir)

    if extract_dir.exists():
        log('Extracted directory exists, skipping unzip: ' + str(extract_dir))
        return extract_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    cmd = ['unzip', str(zipname), '-d', str(extract_dir)]
    proc = subprocess.Popen(cmd)
    proc.communicate()
    if proc.returncode:
        extract_dir.rmtree(extract_dir)
    return extract_dir
