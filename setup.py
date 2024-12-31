import time
from distutils.core import setup

setup(
  name='SQLiteDB',
  py_modules=['sqlitedb'],
  version=time.strftime('%Y%m%d'),
  description='Thin SQLite wrapper to keep a backup on S3 like Object Store',
  long_description='Thin SQLite wrapper to keep a backup by S3 like Object Store',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/SQLiteDB',
  keywords=['sqlite', 's3', 'replication', 'backup']
)
