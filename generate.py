import shutil
from os import path
from typing import List, Tuple
from urllib.request import urlopen
import subprocess

import click

here = path.abspath(path.dirname(__file__))

PACKAGE = 'hmsclient'
SUBPACKAGE = 'genthrift'
generated = 'gen-py'

FB303_URL = 'https://raw.githubusercontent.com/apache/thrift/0.11.0/contrib/fb303/if/fb303.thrift'
METASTORE_URL = 'https://raw.githubusercontent.com/apache/hive/rel/release-2.3.3/metastore/if/hive_metastore.thrift'

config = {path.join("hive_metastore", "ttypes.py"): [('import fb303.ttypes',
                                                      'from ..fb303 import ttypes')],
          path.join("hive_metastore", "ThriftHiveMetastore.py"):
              [('import fb303.FacebookService', 'from .. import fb303')],
          path.join("hive_metastore", "ThriftHiveMetastore-remote"):
              [('from hive_metastore import ThriftHiveMetastore',
                'from ..hive_metastore import ThriftHiveMetastore'),
               ('from hive_metastore.ttypes import *',
                'from ..hive_metastore.ttypes import *')],
          path.join("hive_metastore", "constants.py"): [],
          path.join("hive_metastore", "__init__.py"): [],
          path.join("fb303", "__init__.py"): [(']', ']\nfrom . import FacebookService')],
          path.join("fb303", "FacebookService.py"):
              [('from fb303 import FacebookService',
                'from . import FacebookService'),
               ('from fb303.types import *',
                'from .ttypes import *')],
          path.join("fb303", "constants.py"): [],
          path.join("fb303", "FacebookService-remote"):
          [('from fb303 import FacebookService', 'from . import FacebookService'),
           ('from fb303.ttypes import *', 'from .ttypes import *')],
          path.join("fb303", "ttypes.py"): [],
          '__init__.py': []}


def replace(file_path: str, replacements: List[Tuple[str, str]]) -> str:
    with open(file_path, 'r') as f:
        string = f.read()
    for old, new in replacements:
        string = string.replace(old, new)

    return string


def write_file(string: str, file_path: str) -> None:
    with open(file_path, 'w') as f:
        f.write(string)


def save_url(url):
    data = urlopen(url).read()
    file_path = path.join(here, url.rsplit('/', 1)[-1])
    with open(file_path, 'wb') as f:
        f.write(data)


@click.command()
@click.option('--fb303_url', default=FB303_URL, help='The URL where the fb303.thrift file can be '
                                                     'downloaded')
@click.option('--metastore_url', default=METASTORE_URL, help='The URL where the '
                                                             'hive_metastore.thrift file can be '
                                                             'downloaded')
@click.option('--package', default=PACKAGE, help='The package where the client should be placed')
@click.option('--subpackage', default=SUBPACKAGE, help='The subpackage where the client should be '
                                                    'placed')
def main(fb303_url, metastore_url, package, subpackage):
    for url in (fb303_url, metastore_url):
        save_url(url)
    metastore_path = path.join(here, metastore_url.rsplit('/', 1)[-1])
    metastore_content = replace(metastore_path,
                                [('include "share/fb303/if/fb303.thrift"',
                                  'include "fb303.thrift"')])
    with open(metastore_path, 'w') as f:
        f.write(metastore_content)
    subprocess.call(['thrift', '-r', '--gen', 'py', metastore_path])

    for file_path, replacement in config.items():
        to_write = replace(path.join(here, generated, file_path), replacement)
        write_file(to_write, path.join(here, package, subpackage, file_path))

    shutil.rmtree(generated)


if __name__ == '__main__':
    main()