#!/usr/bin/env python
import os
from setuptools import convert_path, setup, find_packages

NAME  = 'grib_downloader'
DESC  = 'Utitlity for downloading full, or parital, GRIB files'
AUTH  = 'Kyle R. Wodzicki'
EMAIL = 'krwodzicki@gmail.com'
URL   = ''

main_ns  = {}
ver_path = convert_path( os.path.join( NAME, 'version.py') )
with open(ver_path) as ver_file:
  exec(ver_file.read(), main_ns)

setup(
  name             = NAME, 
  description      = DESC,
  url              = URL,
  author           = AUTH,
  author_email     = EMAIL,
  version          = main_ns['__version__'],
  packages         = find_packages(),
  install_requires = [ "fastener" ],
  scripts          = [ os.path.join( '.', 'bin', 'grib_utils' )],
  zip_safe         = False
)
