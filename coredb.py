import sys
import json
import base64
import sqlite3
import logging
import argparse
from logging import critical as log


PYTYPES = dict(i=(int,), f=(int, float), t=(str,), b=(str,))
SQLTYPES = dict(i='int', f='float', t='text', b='blob')


def validate_types(values):
    params = dict()

    for k, v in values.items():
        if v is not None:
            if type(v) not in PYTYPES[k[0]]:
                raise Exception('Invalid type for {}'.format(k))

        if 'b' == k[0] and v is not None:
            params[k] = base64.b64decode(v)
        else:
            params[k] = v

    return params


def execute(conn, sql, params=dict()):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        log('modified(%d) %s', cur.rowcount, sql)
    except Exception:
        log(sql)
        raise

    cur.close()


class Database:
    def __init__(self, db):
        self.conn = sqlite3.connect(db)

    def __del__(self):
        if self.conn:
            self.conn.rollback()
            self.conn.close()

    def commit(self):
        self.conn.commit()

    def create(self, table, primary_key):
        pk = list()
        for k in primary_key:
            pk.append('{} {}'.format(k, SQLTYPES[k[0]]))

        pk = ', '.join(pk)
        pk_constraint = ', '.join(primary_key)

        sql = 'create table {} ({}, primary key({}))'
        execute(self.conn, sql.format(table, pk, pk_constraint))

    def add(self, table, column):
        sql = 'alter table {} add column {} {}'
        execute(self.conn, sql.format(table, column, SQLTYPES[column[0]]))

    def rename(self, table, src_col, dst_col):
        if src_col[0] != dst_col[0]:
            raise Exception('DST column type should be same as SRC')

        sql = 'alter table {} rename column {} to {}'
        execute(self.conn, sql.format(table, src_col, dst_col))

    def drop(self, table, column):
        sql = 'alter table {} drop column {}'
        execute(self.conn, sql.format(table, column))

    def insert(self, table, values):
        cols = values.keys()
        params = validate_types(values)

        first = ','.join(cols)
        second = ','.join([':{}'.format(c) for c in cols])

        sql = 'insert into {}({}) values({})'
        execute(self.conn, sql.format(table, first, second), params)

    def update(self, table, set_dict, where_dict):
        set_dict = validate_types(set_dict)
        where_dict = validate_types(where_dict)

        params = dict()
        params.update({'set_'+k: v for k, v in set_dict.items()})
        params.update({'where_'+k: v for k, v in where_dict.items()})

        first = ', '.join('{}=:set_{}'.format(k, k) for k in set_dict)
        second = ' and '.join('{}=:where_{}'.format(k, k) for k in where_dict)

        sql = 'update {} set {} where {}'
        execute(self.conn, sql.format(table, first, second), params)

    def delete(self, table, where):
        params = validate_types(where)
        where = ' and '.join('{}=:{}'.format(k, k) for k in params)

        sql = 'delete from {} where {}'
        execute(self.conn, sql.format(table, where), params)


def process_transaction(db, transaction):
    for op in transaction:
        cmd, table = op['op'], op['table']

        if 'insert' == cmd:
            db.insert(table, op['values'])

        if 'update' == cmd:
            db.update(table, op['values'], op['where'])

        if 'delete' == cmd:
            db.delete(table, op['where'])


def main(args):
    db = Database(args.db)

    if 'create' == args.operation:
        db.create(args.table, args.primary_key.split(','))

    elif 'add' == args.operation:
        db.add(args.table, args.column)

    elif 'rename' == args.operation:
        db.rename(args.table, args.src, args.dst)

    elif 'drop' == args.operation:
        db.drop(args.table, args.column)

    elif 'insert' == args.operation:
        db.insert(args.table, json.loads(sys.stdin.read()))

    elif 'update' == args.operation:
        obj = json.loads(sys.stdin.read())
        db.update(args.table, obj['set'], obj['where'])

    elif 'delete' == args.operation:
        db.delete(args.table, json.loads(sys.stdin.read()))

    else:
        raise Exception('Invalid Operation : {}'.format(args.operation))

    db.commit()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument(
        '--config', default='config.json',
        help='Object bucket configuration')

    ARGS.add_argument('--db', help='Database Name')
    ARGS.add_argument('--table', help='Table Name')
    ARGS.add_argument('operation', help='Operation to be done')

    ARGS.add_argument('--src', help='Old column name')
    ARGS.add_argument('--dst', help='New column name')
    ARGS.add_argument('--column', help='Column name')
    ARGS.add_argument('--primary_key', help='Comma separated column list')

    ARGS = ARGS.parse_args()

    main(ARGS)
