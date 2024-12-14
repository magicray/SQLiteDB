import sys
import json
import time
import base64
import sqlite3
import logging
import argparse
from logging import critical as log


TYPES = dict(i='int', f='float', t='text', b='blob')
SQL = dict(
    add='alter table {} add column {} {}',
    drop='alter table {} drop column {}',
    rename='alter table {} rename column {} to {}',
    create='create table {} ({}, primary key({}))',
    insert='insert into {}({}) values({})',
    update='update {} set {} where {}',
    delete='delete from {} where {}')


def validate_types(values):
    params = dict()

    for k, v in values.items():
        params[k] = v

        if v is not None:
            if 'i' == k[0] and type(v) is not int:
                raise Exception('Invalid type for {}'.format(k))
            elif 'f' == k[0] and type(v) not in (int, float):
                raise Exception('Invalid type for {}'.format(k))
            elif 't' == k[0] and type(v) is not str:
                raise Exception('Invalid type for {}'.format(k))
            elif 'b' == k[0]:
                if type(v) is not str:
                    raise Exception('Invalid type for {}'.format(k))
                elif v is not None:
                    params[k] = base64.b64decode(v)

    return params


def process_transaction(db, transaction):
    for op in transaction:
        if op['op'] in ('create', 'add', 'rename', 'drop'):
            if len(transaction) != 1:
                raise Exception('Only one DDL statement is allowed')

        elif op['op'] not in ('insert', 'update', 'delete'):
            raise Exception('Invalid Operation - {}'.format(op['op']))

    conn = sqlite3.connect(db)
    cur = conn.cursor()

    for op in transaction:
        sql, params, table = None, dict(), op['table']

        if 'create' == op['op']:
            pk = list()
            for k in op['primary_key']:
                pk.append('{} {}'.format(k, TYPES[k[0]]))

            pk = ', '.join(pk)
            pk_constraint = ', '.join(op['primary_key'])

            sql = SQL['create'].format(table, pk, pk_constraint)

        elif 'add' == op['op']:
            sql = SQL['add'].format(table, op['column'], TYPES[op['column'][0]])

        elif 'rename' == op['op']:
            if op['src'][0] != op['dst'][0]:
                raise Exception('DST column type should be same as SRC')

            sql = SQL['rename'].format(table, op['src'], op['dst'])

        elif 'drop' == op['op']:
            sql = SQL['drop'].format(table, op['column'])

        elif 'insert' == op['op']:
            cols = op['values'].keys()
            params = validate_types(op['values'])

            first = ','.join(cols)
            second = ','.join([':{}'.format(c) for c in cols])

            sql = SQL['insert'].format(table, first, second)

        elif 'update' == op['op']:
            set_dict = validate_types(op['values'])
            where_dict = validate_types(op['where'])

            params.update({'set_'+k: v for k, v in set_dict.items()})
            params.update({'where_'+k: v for k, v in where_dict.items()})

            first = ', '.join('{}=:set_{}'.format(k, k) for k in set_dict)
            second = ' and '.join('{}=:where_{}'.format(k, k) for k in where_dict)

            sql = SQL['update'].format(table, first, second)

        elif 'delete' == op['op']:
            params = validate_types(op['where'])
            where = ' and '.join('{}=:{}'.format(k, k) for k in params)

            sql = SQL['delete'].format(table, where)

        log(sql)
        cur.execute(sql, params)
        log('modified(%d)', cur.rowcount)

    cur.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument(
        '--config', default='config.json',
        help='Object bucket configuration')

    ARGS.add_argument('--db', help='Database Name')

    ARGS = ARGS.parse_args()

    process_transaction(ARGS.db, json.loads(sys.stdin.read()))
