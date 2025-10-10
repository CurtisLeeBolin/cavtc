#!/usr/bin/env python3
#
#  cavtc.py
#
#  Copyright 2025 Curtis Lee Bolin <CurtisLeeBolin@gmail.com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#

import os
import sqlite3
import subprocess
import time
import sys
import socket
import avtc
import time
import secrets


def create_db(db_file):
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    cursor.execute(
        '''
        CREATE TABLE queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            working_dir TEXT NOT NULL,
            absolute_filename TEXT NOT NULL
        );
        '''
    )
    cursor.execute(
        'INSERT INTO queue (id, working_dir, absolute_filename) VALUES (50000, "_", "_");'
    )
    cursor.execute(
        'DELETE FROM queue WHERE id = 50000;'
    )
    cursor.execute(
        '''
        CREATE TABLE running (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created TIMESTAMP NOT NULL,
            working_dir TEXT NOT NULL,
            absolute_filename TEXT NOT NULL,
            hostname TEXT NOT NULL
        );
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE completed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started TIMESTAMP NOT NULL,
            completed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created TIMESTAMP NOT NULL,
            working_dir TEXT NOT NULL,
            absolute_filename TEXT NOT NULL,
            hostname TEXT NOT NULL
        );
        '''
    )
    cursor.execute(
        '''
        CREATE TABLE failed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            failed TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created TIMESTAMP NOT NULL,
            working_dir TEXT NOT NULL,
            absolute_filename TEXT NOT NULL,
            hostname TEXT NOT NULL,
            return_msg TEXT NOT NULL
        );
        '''
    )
    connection.commit()
    connection.close()


def get_rows(db_file, table):
    table_tuple = ('queue', 'running', 'completed', 'failed')
    if table in table_tuple:
        sql_str = f'SELECT * FROM {table}'
        connection = sqlite3.connect(db_file, timeout=30.0)
        cursor = connection.cursor()
        rows = cursor.execute(sql_str).fetchall()
        connection.close()
        return rows
    else:
        raise Exception(f'Table `{table}` not found')


def get_row(db_file, table):
    table_tuple = ('queue', 'running', 'completed', 'failed')
    if table in table_tuple:
        sql_str = f'SELECT * FROM {table}'
        connection = sqlite3.connect(db_file, timeout=30.0)
        cursor = connection.cursor()
        row = cursor.execute(sql_str).fetchone()
        connection.close()
        return row
    else:
        raise Exception(f'Table `{table}` not found')


def get_queue_id_list(db_file):
    sql_str = 'SELECT id FROM queue'
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    rows = cursor.execute(sql_str).fetchall()
    connection.close()
    return [row[0] for row in rows]


def get_next_video(db_file):
    sql_str_select = 'SELECT id, created, working_dir, absolute_filename FROM queue'
    sql_str_add = 'INSERT INTO running(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
    sql_str_del = 'DELETE FROM queue WHERE id = ?'
    sql_str_select_id_running = 'SELECT last_insert_rowid()'
    sql_str_select_running = 'SELECT id, started, created, working_dir, absolute_filename, hostname FROM running WHERE id = ?'
    id_running = 0
    id = 0
    started = ''
    created = ''
    working_dir = ''
    absolute_filename = ''
    hostname = socket.gethostname()
    connection = sqlite3.connect(db_file, timeout=30.0)
    connection.isolation_level = None
    cursor = connection.cursor()
    cursor.execute('BEGIN TRANSACTION')
    try:
        id, created, working_dir, absolute_filename = cursor.execute(sql_str_select).fetchone()
        cursor.execute(sql_str_add, (created, working_dir, absolute_filename, hostname))
        id_running = cursor.execute(sql_str_select_id_running).fetchone()[0]
        cursor.execute(sql_str_del, (id,))
        id, started, created, working_dir, absolute_filename, hostname = cursor.execute(sql_str_select_running, (id_running,)).fetchone()
        connection.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        connection.rollback()
    connection.close()
    return (id, started, created, working_dir, absolute_filename, hostname)


def retry(db_file, table):
    sql_str_select = f'SELECT id, working_dir, absolute_filename FROM {table}'
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    rows = cursor.execute(sql_str_select).fetchall()
    connection.close()
    rows_list = list(rows)
    data_list = []
    for row in rows_list:
        _, working_dir, absolute_filename = row
        data_list.append([working_dir, absolute_filename])
    add_rows_lowest_id(db_file, 'queue', data_list)
    print(f'Deleting from table {table}:')
    for id, working_dir, absolute_filename in rows_list:
        print(f'  {id=}\n  {working_dir=}\n  {absolute_filename=}\n')
        del_row(db_file, table, id)


def add_rows_lowest_id(db_file, table, data_list):
    id_list = get_queue_id_list(db_file)
    sql_str_add = 'INSERT INTO queue(id, working_dir, absolute_filename) VALUES (?, ?, ?)'
    connection = sqlite3.connect(db_file, timeout=30.0)
    connection.isolation_level = None
    cursor = connection.cursor()
    if data_list != []:
        for row in data_list:
            next_number = find_first_missing_number(id_list)
            working_dir, absolute_filename = row
            cursor.execute('BEGIN TRANSACTION')
            try:
                cursor.execute(sql_str_add, (next_number, working_dir, absolute_filename))
                id_list.append(next_number)
                connection.commit()
            except sqlite3.Error as e:
                print(f"Error: {e}")
                connection.rollback()
    else:
        sys.exit(f'Error: Table {table} is empty.')
    connection.close()


def add_row(db_file, table, data):
    sql_str = ''
    if table == 'queue':
        sql_str = 'INSERT INTO queue(working_dir, absolute_filename) VALUES (?, ?)'
    elif table == 'running':
        sql_str = 'INSERT INTO running(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
    elif table == 'completed':
        sql_str = 'INSERT INTO completed(started, created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?, ?)'
    elif table == 'failed':
        sql_str = 'INSERT INTO failed(created, working_dir, absolute_filename, hostname, return_msg) VALUES (?, ?, ?, ?, ?)'
    else:
        raise Exception(f'Table `{table}` not found')
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    cursor.execute(sql_str, data)
    connection.commit()
    connection.close()


def add_rows(db_file, table, data_list):
    sql_str = ''
    if table == 'queue':
        sql_str = 'INSERT INTO queue(working_dir, absolute_filename) VALUES (?, ?)'
    elif table == 'running':
        sql_str = 'INSERT INTO running(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
    elif table == 'completed':
        sql_str = 'INSERT INTO completed(started, created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?, ?)'
    elif table == 'failed':
        sql_str = 'INSERT INTO failed(created, working_dir, absolute_filename, hostname, return_msg) VALUES (?, ?, ?, ?, ?)'
    else:
        raise Exception(f'Table `{table}` not found')
    connection = sqlite3.connect(db_file, timeout=30.0)
    connection.isolation_level = None
    cursor = connection.cursor()
    cursor.execute('BEGIN TRANSACTION')
    try:
        cursor.executemany(sql_str, data_list)
        connection.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        connection.rollback()
    connection.close()


def del_row(db_file, table, id):
    table_tuple = ('queue', 'running', 'completed', 'failed')
    if table in table_tuple:
        sql_str = f'DELETE FROM {table} WHERE id = ?'
        connection = sqlite3.connect(db_file, timeout=30.0)
        cursor = connection.cursor()
        cursor.execute(sql_str, (id,))
        connection.commit()
        connection.close()
    else:
        raise Exception(f'Table `{table}` not found')


def reset_table(db_file, table):
    table_tuple = ('queue', 'running', 'completed', 'failed')
    if table in table_tuple:
        sql_str = f'DELETE FROM {table}'
        connection = sqlite3.connect(db_file, timeout=30.0)
        cursor = connection.cursor()
        cursor.execute(sql_str)
        cursor.execute('DELETE FROM SQLITE_SEQUENCE WHERE name = ?', (table,))
        connection.commit()
        connection.close()
    else:
        raise Exception(f'Table `{table}` not found')


def find_first_missing_number(number_list):
    number_list.sort()
    i = 1
    while True:
        if i not in number_list:
            return i
        i += 1


def server(db_file):
    while True:
        id = 0
        started = ''
        created = ''
        working_dir = ''
        absolute_filename = ''
        hostname = ''
        try:
            id, started, created, working_dir, absolute_filename, hostname = get_next_video(db_file)
        except:
            time.sleep(secrets.randbelow(6) + 5)
            continue
        tc = avtc.AudioVideoTransCoder([],disable_lockfile=True)
        return_msg = tc.transcode(absolute_filename, working_dir)
        if return_msg == None:
            add_row(db_file, 'completed', (started, created, working_dir, absolute_filename, hostname))
            del_row(db_file, 'running', id)
            print()
        else:
            add_row(db_file, 'failed', (created, working_dir, absolute_filename, hostname, return_msg))
            del_row(db_file, 'running', id)
            print(f'{return_msg=}')
            print()


def main():
    import argparse

    home = os.environ.get('HOME')
    working_dir = os.getcwd()
    config_dir = f'{home}/staging'
    db_file = f'{config_dir}/.cavtc.db'
    mode_list = [
        'files',
        'server',
        'show',
        'recursive',
        'reset',
        'retry',
        'rmid'
    ]

    os.makedirs(config_dir, exist_ok=True)

    if not os.path.isfile(db_file):
        create_db(db_file)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        dest='mode',
        nargs='*',
        metavar='mode',
        help='mode'
    )
    args = parser.parse_args()

    if len(args.mode) == 0:
        parser.parse_args(['--help'])
    else:
        if args.mode[0] == 'files':
            tc = avtc.AudioVideoTransCoder([])
            list_dir = os.listdir(working_dir)
            list_dir.sort()
            data_list = []
            for file in list_dir:
                filename_full, file_ext = os.path.splitext(file)
                file_ext = file_ext[1:]
                if tc.check_file_type(file_ext):
                    absolute_filename = os.path.join(working_dir, file)
                    data = (working_dir, absolute_filename)
                    data_list.append(data)
            if len(args.mode) == 1:
                add_rows(db_file, 'queue', data_list)
            elif len(args.mode) == 2:
                if args.mode[1] == 'lowest':
                    add_rows_lowest_id(db_file, 'queue', data_list)
                else:
                    parser.parse_args(['--help'])
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'recursive':
            exempt_list = ('0in', '0out')
            tc = avtc.AudioVideoTransCoder([])
            data_list = []
            for root, dirs, files in os.walk(working_dir):
                for file in files:
                    filename_full, file_ext = os.path.splitext(file)
                    file_ext = file_ext[1:]
                    if tc.check_file_type(file_ext) and not any(s in root for s in exempt_list):
                        absolute_filename = os.path.join(root, file)
                        data = (working_dir, absolute_filename)
                        data_list.append(data)
            if len(args.mode) == 1:
                add_rows(db_file, 'queue', data_list)
            elif len(args.mode) == 2:
                if args.mode[1] == 'lowest':
                    add_rows_lowest_id(db_file, 'queue', data_list)
                else:
                    parser.parse_args(['--help'])
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'server':
            if len(args.mode) == 1:
                server(db_file)
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'show':
            table_tuple = ('queue', 'running', 'completed', 'failed')
            if len(args.mode) != 2:
                parser.parse_args(['--help'])
            elif args.mode[1] in table_tuple:
                for row in get_rows(db_file, args.mode[1]):
                    row_strings = [str(x) for x in row]
                    print('|'.join(row_strings))
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'retry':
            table_tuple = ('running', 'failed')
            if len(args.mode) != 2:
                parser.parse_args(['--help'])
            elif args.mode[1] in table_tuple:
                retry(db_file, args.mode[1])
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'rmid':
            table_tuple = ('queue', 'running', 'completed', 'failed')
            if len(args.mode) != 3:
                parser.parse_args(['--help'])
            elif args.mode[1] in table_tuple:
                del_row(db_file, args.mode[1], args.mode[2])
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'reset':
            table_tuple = ('queue', 'running', 'completed', 'failed')
            if len(args.mode) != 2:
                parser.parse_args(['--help'])
            elif args.mode[1] in table_tuple:
                reset_table(db_file, args.mode[1])
            else:
                parser.parse_args(['--help'])

        else:
            parser.parse_args(['--help'])


if __name__ == "__main__":
    main()
