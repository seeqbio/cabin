import os
import csv
import gzip
import pysam
import subprocess
from lxml import etree
from Bio import SeqIO
from ftplib import FTP
from dateutil import parser
from pathlib import Path

from . import logger, settings, BiodbError


def read_xsv(path, delimiter='\t', columns=None, header_leading_hash=True, ignore_leading_hash=False, gzipped=False, encoding=None):
    """
    Parses a delimiter separated text file and yields rows as dictionaries.

    Args:
        delimiter (str):        column delimiter.
        columns   (list|None):  If a list is given it is assumed that all lines
                                of the source file are content lines (yielded
                                as rows).  If None it is expected that the
                                first line of the file (the "header line")
                                defines the column names.
        header_leading_hash (bool): Whether the header line has a leading `#`;
                                ignored if `columns` is given.
        ignore_leading_hash:    ignores lines with leading # from file contents.
        gzipped (bool):         Whether the given file is gzipped.
    """
    path = str(path)
    f = gzip.open(path, 'rt') if gzipped else open(path, 'r', encoding=encoding)

    logger.info('reading records from "{p}"'.format(p=f.name))

    if columns is None:
        header = f.readline().strip()
        if header_leading_hash:
            if header[0] != '#':
                raise BiodbError('Expected first line to start with #')
            header = header[1:]
        columns = header.split(delimiter)

    for line in f:
        if ignore_leading_hash and line.startswith('#'):
            continue
        values = line.strip().split(delimiter)
        yield dict(zip(columns, values))

    f.close()


def read_csv(path, delimiter=',', quotechar='"', encoding=None):
    """
    Uses csv library to parse csv in the event that simple delimiter split is not
    enough. For example, some cells contain a citation, with a ',' in it, that
    is in quotes. eg:  "Flex E, et al. Somatically acquired JAK1".
    """
    with open(path, encoding=encoding) as infile:
        csv_content = csv.reader(infile, delimiter=delimiter, quotechar=quotechar)
        header = next(csv_content)

        for row in csv_content:
            yield dict(zip(header, row))


def read_vcf(path):
    path = str(path)
    logger.info('reading VCF records from "{p}"'.format(p=path))
    vf = pysam.VariantFile(path)
    for variant in vf.fetch():
        row = {
            'CHROM': variant.chrom,
            'POS': variant.pos,
            'ID': variant.id,
            'REF': variant.ref,
            'ALT': variant.alts,         # tuple: ('A',)
            'QUAL': variant.qual,
            'FILTER': variant.filter,
            'INFO': variant.info,        # eg usage: info.get('CLNDISDB')
        }
        yield row


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


def read_obo(path):
    """ For each term in ontology, yields the name and id of the the term,
    the immediate children term, and immediate parent terms. All xrefs are
    kept and user can parse for a subset by prefix. Source: obo format. """

    from pronto import Ontology
    # FIXME: UnicodeWarning: unsound encoding, assuming ISO-8859-1 (73% confidence)
    # with hpo import, not mondo or disease ontology

    ontology = Ontology(str(path))

    for term in ontology.terms():

        children = [child.id for child in ontology[term.id].subclasses(distance=1, with_self=False)]
        parents = [parent.id for parent in ontology[term.id].superclasses(distance=1, with_self=False)]

        yield {
            '_term': term,
            'name': term.name,
            'id': term.id,
            'def': term.definition,
            'children': children,
            'parents': parents,
            'xrefs': [xref.id for xref in term.xrefs]
        }


def read_fasta(path, gzipped=False):
    f = gzip.open(path, 'rt') if gzipped else open(path, 'r')
    for record in SeqIO.parse(f, 'fasta'):
        yield record.id, str(record.seq)

    f.close()


def wget(source, destination):
    cmd = ['wget', '-q', str(source), '-O', str(destination)]
    if not settings.SGX_NON_INTERACTIVE:
        cmd = cmd + ['--show-progress']
    proc = subprocess.Popen(cmd)
    proc.communicate()
    if proc.returncode == 0:
        logger.info('Successfully downloaded to %s' % destination)
    else:
        os.remove(destination)
        raise BiodbError('Failed to download to %s' % destination)


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


def cut_tsv_with_zcat(src, dst):
    """ creates a file with first 5 columns only, eg: partial vcf for dbSNP"""
    command = 'zcat {src} | cut -f 1-5,8 > {dst}'.format(src=src, dst=dst)
    proc = subprocess.Popen(command, shell=True)
    proc.communicate()
    if proc.returncode:
        raise BiodbError('Failed to cut: ' + str(src))


def gunzip(src, dst):
    command = 'gunzip -c {src} > {dst}'.format(src=src, dst=dst)
    proc = subprocess.Popen(command, shell=True)
    proc.communicate()
    if proc.returncode:
        raise BiodbError('Failed to unzip: ' + str(src))


def unzip(zipname, extract_dir=None):
    logger.info('Unzipping: ' + str(zipname))
    zipname = Path(zipname)
    if extract_dir is None:
        assert Path(zipname).suffix == '.zip', 'expected zip file name to end with ".zip"'
        extract_dir = zipname.parent / zipname.stem
    else:
        extract_dir = Path(extract_dir)

    if extract_dir.exists():
        logger.info('Extracted directory exists, skipping unzip: ' + str(extract_dir))
        return extract_dir

    extract_dir.mkdir(parents=True, exist_ok=True)
    cmd = ['unzip', str(zipname), '-d', str(extract_dir)]
    proc = subprocess.Popen(cmd)
    proc.communicate()
    if proc.returncode:
        extract_dir.rmtree(extract_dir)
    return extract_dir
