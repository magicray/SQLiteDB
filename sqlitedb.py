import sys
import json
import time
import boto3
import base64
import sqlite3
import logging
import argparse
from logging import critical as log


class S3Bucket:
    def __init__(self, s3bucket, key_id, secret_key):
        tmp = s3bucket.split('/')
        self.bucket = tmp[-1]
        self.endpoint = '/'.join(tmp[:-1])

        self.s3 = boto3.client('s3', endpoint_url=self.endpoint,
                               aws_access_key_id=key_id,
                               aws_secret_access_key=secret_key)

    def get(self, key):
        ts = time.time()

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.NoSuchKey:
            return None

        octets = obj['Body'].read()
        assert (len(octets) == obj['ContentLength'])
        log('s3(%s) get(%s/%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(octets),
            (time.time()-ts) * 1000)
        return octets

    def put(self, key, value):
        ts = time.time()
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=value,
                           IfNoneMatch='*')
        log('s3(%s) put(%s/%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(value),
            (time.time()-ts) * 1000)


class CoreDB:
    def __init__(self, db, s3bucket, s3_auth_key, s3_auth_secret):
        self.db = db
        self.lsn = None
        self.txns = list()

        self.conn = sqlite3.connect(db + '.sqlite3')
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.execute('''create table if not exists _kv(
                                 key   text primary key,
                                 value text)''')
        self.conn.execute("""insert or ignore into _kv(key, value)
                             values('lsn', 0)""")

        self.s3 = S3Bucket(s3bucket, s3_auth_key, s3_auth_secret)
        self.sync()

    def __del__(self):
        if self.conn:
            self.conn.rollback()
            self.conn.close()

    def commit(self):
        lsn, self.lsn = self.lsn, None
        txns, self.txns = self.txns, None

        TYPES = dict(i=int, f=float, t=str, b=bytes)
        for sql, params in txns:
            for k, v in params.items():
                assert (type(v) is TYPES[k[0]])

                if 'b' == k[0]:
                    params[k] = base64.b64encode(v).decode()

        octets = json.dumps(txns, indent=4, sort_keys=True)

        self.s3.put('SQLiteDB/{}/logs/{}'.format(self.db, lsn+1), octets)
        self.conn.execute("update _kv set value=? where key='lsn'", [lsn+1])
        self.conn.commit()

        self.lsn, self.txns = lsn+1, list()

    def modify(self, sql, params=dict()):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        count = cur.rowcount
        cur.close()

        self.txns.append((sql, params))
        log('modified(%d) %s', count, sql)

    def sync(self):
        rows = self.read("select value from _kv where key='lsn'")
        self.lsn = int(rows[0][0])

        while True:
            octets = self.s3.get('SQLiteDB/{}/logs/{}'.format(
                                 self.db, self.lsn+1))
            if octets is None:
                break

            txns = json.loads(octets)

            TYPES = dict(i=int, f=float, t=str, b=bytes)
            for sql, params in txns:
                for k, v in params.items():
                    if 'b' == k[0]:
                        params[k] = base64.b64decode(v.encode())

                    assert (type(params[k]) is TYPES[k[0]])

            cur = self.conn.cursor()

            for sql, params in txns:
                cur.execute(sql, params)
                log('applied(%d) %s', self.lsn+1, sql)

            self.conn.execute("update _kv set value=? where key='lsn'",
                              [self.lsn+1])
            self.conn.commit()
            self.lsn += 1

        log('initialized(%s.sqlite3) lsn(%d)', self.db, self.lsn)

    def read(self, sql, params=dict()):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()

        log('fetched(%d) %s', len(rows), sql)
        return rows


class Database:
    def __init__(self, db, s3bucket, s3_auth_key, s3_auth_secret):
        self.db = CoreDB(db, s3bucket, s3_auth_key, s3_auth_secret)

        self.SQLTYPES = dict(i='int', f='float', t='text', b='blob')

    def commit(self):
        self.db.commit()

    def validate_types(self, values):
        PYTYPES = dict(i=(int,), f=(int, float), t=(str,), b=(str, bytes))

        params = dict()
        for k, v in values.items():
            if v is not None:
                if type(v) not in PYTYPES[k[0]]:
                    raise Exception('Invalid type for {}'.format(k))

            if 'b' == k[0] and type(v) is str:
                params[k] = base64.b64decode(v)
            else:
                params[k] = v

        return params

    def create_table(self, table, primary_key):
        col = ['{} {} not null'.format(k, self.SQLTYPES[k[0]])
               for k in primary_key]

        self.db.modify('create table {} ({}, primary key({}))'.format(
            table, ', '.join(col), ', '.join(primary_key)))

    def drop_table(self, table):
        self.db.modify('drop table {}'.format(table))

    def add_column(self, table, column):
        self.db.modify('alter table {} add column {} {}'.format(
            table, column, self.SQLTYPES[column[0]]))

    def rename_column(self, table, src_col, dst_col):
        if src_col[0] != dst_col[0]:
            raise Exception('DST column type should be same as SRC')

        self.db.modify('alter table {} rename column {} to {}'.format(
            table, src_col, dst_col))

    def drop_column(self, table, column):
        self.db.modify('alter table {} drop column {}'.format(table, column))

    def insert(self, table, row):
        params = self.validate_types(row)
        placeholders = [':{}'.format(k) for k in row]

        self.db.modify('insert into {}({}) values({})'.format(
            table, ', '.join(row), ', '.join(placeholders)), params)

    def update(self, table, set_dict, where_dict):
        set_dict = self.validate_types(set_dict)
        where_dict = self.validate_types(where_dict)

        params = dict()
        params.update({k+'_set': v for k, v in set_dict.items()})
        params.update({k+'_where': v for k, v in where_dict.items()})

        first = ', '.join('{}=:{}_set'.format(k, k) for k in set_dict)
        second = ' and '.join('{}=:{}_where'.format(k, k) for k in where_dict)

        self.db.modify('update {} set {} where {}'.format(
            table, first, second), params)

    def delete(self, table, where):
        params = self.validate_types(where)
        where = ' and '.join('{}=:{}'.format(k, k) for k in params)

        self.db.modify('delete from {} where {}'.format(table, where), params)


def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    args = argparse.ArgumentParser()

    args.add_argument(
        '--config', default='config.json',
        help='Object bucket configuration')

    args.add_argument('--db', help='Database Name')
    args.add_argument('--table', help='Table Name')
    args.add_argument('operation', help='Operation to be done',
                      choices=['create_table', 'drop_table', 'add_column',
                               'rename_column', 'drop_column',
                               'insert', 'update', 'delete', 'sync'])

    args.add_argument('--src', help='Old column name')
    args.add_argument('--dst', help='New column name')
    args.add_argument('--column', help='Column name')
    args.add_argument('--primary_key', help='Comma separated column list')

    args = args.parse_args()

    with open(args.config) as fd:
        conf = json.load(fd)

    db = Database(args.db, conf['s3bucket'], conf['s3bucket_auth_key'],
                  conf['s3bucket_auth_secret'])

    if 'create_table' == args.operation:
        db.create_table(args.table, args.primary_key.split(','))

    elif 'drop_table' == args.operation:
        db.drop_table(args.table)

    elif 'add_column' == args.operation:
        db.add_column(args.table, args.column)

    elif 'rename_column' == args.operation:
        db.rename_column(args.table, args.src, args.dst)

    elif 'drop_column' == args.operation:
        db.drop_column(args.table, args.column)

    elif 'insert' == args.operation:
        db.insert(args.table, json.loads(sys.stdin.read()))

    elif 'update' == args.operation:
        obj = json.loads(sys.stdin.read())
        db.update(args.table, obj, obj.pop('where'))

    elif 'delete' == args.operation:
        db.delete(args.table, json.loads(sys.stdin.read()))

    elif 'sync' == args.operation:
        old = 0
        delay = 1
        while True:
            lsn = db.sync()
            if old == lsn:
                time.sleep(delay)
                delay = min(60, 2*delay)
            old = lsn
    else:
        raise Exception('Invalid Operation : {}'.format(args.operation))

    db.commit()


if __name__ == '__main__':
    main()
