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


def create_db(db_file):
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    cursor.execute(
        '''
        CREATE TABLE queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            working_dir TEXT NOT NULL,
            absolute_filename TEXT NOT NULL,
            running INTEGER CHECK (running IN (0, 1)) NOT NULL,
            hostname TEXT
        );
        '''
    )
    cursor.execute(
        'INSERT INTO queue (id, working_dir, absolute_filename, running) VALUES (50000, "_", "_", 0);'
    )
    cursor.execute(
        'DELETE FROM queue WHERE id = 50000;'
    )
    cursor.execute(
        '''
        CREATE TABLE completed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            hostname TEXT NOT NULL
        );
        '''
    )
    connection.commit()
    connection.close()


def get_rows(db_file, table):
    sql_str = ''
    if table == 'queue':
        sql_str = 'SELECT * FROM queue'
    elif table == 'completed':
        sql_str = 'SELECT * FROM completed'
    elif table == 'failed':
        sql_str = 'SELECT * FROM failed'
    else:
        raise Exception(f'Table `{table}` not found')
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    rows = cursor.execute(sql_str).fetchall()
    connection.close()
    return rows


def get_row(db_file, table):
    sql_str = ''
    if table == 'queue':
        sql_str = 'SELECT id, created, working_dir, absolute_filename, running, hostname FROM queue'
    else:
        raise Exception(f'Table `{table}` not found')
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    row = cursor.execute(sql_str).fetchone()
    connection.close()
    return row


def get_next_video(db_file):
    hostname = socket.gethostname()
    sql_str_select = 'SELECT id, created, working_dir, absolute_filename FROM queue WHERE running = 0'
    sql_str_update = 'UPDATE queue SET running = 1, hostname = ? WHERE id = ?'
    connection = sqlite3.connect(db_file, timeout=30.0)
    connection.isolation_level = None
    cursor = connection.cursor()
    cursor.execute('BEGIN TRANSACTION')
    id = 0
    created = ''
    working_dir = ''
    absolute_filename = ''
    try:
        id, created, working_dir, absolute_filename = cursor.execute(sql_str_select).fetchone()
        cursor.execute(sql_str_update, (hostname, id))
        connection.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        connection.rollback()
    connection.close()
    return (id, created, working_dir, absolute_filename, hostname)


def add_row(db_file, table, data):
    sql_str = ''
    if table == 'queue':
        sql_str = 'INSERT INTO queue(working_dir, absolute_filename, running) VALUES (?, ?, ?)'
    elif table == 'completed':
        sql_str = 'INSERT INTO completed(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
    elif table == 'failed':
        sql_str = 'INSERT INTO failed(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
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
        sql_str = 'INSERT INTO queue(working_dir, absolute_filename, running) VALUES (?, ?, ?)'
    elif table == 'completed':
        sql_str = 'INSERT INTO completed(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
    elif table == 'failed':
        sql_str = 'INSERT INTO failed(created, working_dir, absolute_filename, hostname) VALUES (?, ?, ?, ?)'
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
    sql_str = ''
    if table == 'queue':
        sql_str = 'DELETE FROM queue WHERE id = ?'
    elif table == 'completed':
        sql_str = 'DELETE FROM completed WHERE id = ?'
    elif table == 'failed':
        sql_str = 'DELETE FROM failed WHERE id = ?'
    else:
        raise Exception(f'Table `{table}` not found')
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    cursor.execute(sql_str, (id,))
    connection.commit()
    connection.close()


def reset_table(db_file, table):
    sql_str = ''
    if table == 'queue':
        sql_str = 'DELETE FROM queue'
    elif table == 'completed':
        sql_str = 'DELETE FROM completed'
    elif table == 'failed':
        sql_str = 'DELETE FROM failed'
    else:
        raise Exception(f'Table `{table}` not found')
    connection = sqlite3.connect(db_file, timeout=30.0)
    cursor = connection.cursor()
    cursor.execute(sql_str)
    cursor.execute('DELETE FROM SQLITE_SEQUENCE WHERE name = ?', (table,))
    connection.commit()
    connection.close()


def printOnSameLine(line):
    columns, lines = os.get_terminal_size()
    clear_line_string = ' ' * columns
    line = line.replace('\n', '')
    line = line[:columns]
    print(f'\r{clear_line_string}\r{line}', end='')


def server(db_file):
    while True:
        id = 0
        created = ''
        working_dir = ''
        absolute_filename = ''
        hostname = ''
        try:
            id, created, working_dir, absolute_filename, hostname = get_next_video(db_file)
        except:
            time.sleep(1)
            continue

        tc = avtc.AudioVideoTransCoder([],disable_lockfile=True)
        returncode = tc.transcode(absolute_filename, working_dir)

        if returncode == 0:
            add_row(db_file, 'completed', (created, working_dir, absolute_filename, hostname))
            del_row(db_file, 'queue', id)
            print()
        else:
            add_row(db_file, 'failed', (created, working_dir, absolute_filename, hostname))
            del_row(db_file, 'queue', id)
            print(f'{returncode=}')
            print()


def main():
    import argparse

    home = os.environ.get('HOME')
    working_dir = os.getcwd()
    config_dir = f'{home}/.tmp/'
    db_file = f'{config_dir}/.cavtc.db'
    mode_list = [
        'server',
        'show',
        'rmid',
        'reset',
        'recursive'
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
        tc = avtc.AudioVideoTransCoder([])
        list_dir = os.listdir(working_dir)
        list_dir.sort()
        data_list = []
        for file in list_dir:
            filename_full, file_ext = os.path.splitext(file)
            file_ext = file_ext[1:]
            if tc.check_file_type(file_ext):
                absolute_filename = os.path.join(working_dir, file)
                data = (working_dir, absolute_filename, False)
                data_list.append(data)
        add_rows(db_file, 'queue', data_list)

    else:
        if args.mode[0] == 'recursive':
            if len(args.mode) == 1:
                exempt_list = ('0in', '0out')
                tc = avtc.AudioVideoTransCoder([])
                data_list = []
                for root, dirs, files in os.walk(working_dir):
                    for file in files:
                        filename_full, file_ext = os.path.splitext(file)
                        file_ext = file_ext[1:]
                        if tc.check_file_type(file_ext) and not any(s in root for s in exempt_list):
                            absolute_filename = os.path.join(root, file)
                            data = (working_dir, absolute_filename, False)
                            data_list.append(data)
                add_rows(db_file, 'queue', data_list)
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'server':
            if len(args.mode) == 1:
                server(db_file)
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'show':
            if len(args.mode) != 2:
                parser.parse_args(['--help'])
            elif args.mode[1] == 'queue':
                for row in get_rows(db_file, 'queue'):
                    print(f'{row[0]}|{row[1]}|{row[2]}|{row[3]}|{row[4]}|{row[5]}')
            elif args.mode[1] == 'completed':
                for row in get_rows(db_file, 'completed'):
                    print(f'{row[0]}|{row[1]}|{row[2]}|{row[3]}|{row[4]}|{row[5]}')
            elif args.mode[1] == 'failed':
                for row in get_rows(db_file, 'failed'):
                    print(f'{row[0]}|{row[1]}|{row[2]}|{row[3]}|{row[4]}|{row[5]}')
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'rmid':
            if len(args.mode) != 3:
                parser.parse_args(['--help'])
            elif args.mode[1] == 'queue':
                del_row(db_file, 'queue', args.mode[2])
            elif args.mode[1] == 'completed':
                del_row(db_file, 'completed', args.mode[2])
            elif args.mode[1] == 'failed':
                del_row(db_file, 'failed', args.mode[2])
            else:
                parser.parse_args(['--help'])

        elif args.mode[0] == 'reset':
            if len(args.mode) != 2:
                parser.parse_args(['--help'])
            elif args.mode[1] == 'queue':
                reset_table(db_file, 'queue')
            elif args.mode[1] == 'completed':
                reset_table(db_file, 'completed')
            elif args.mode[1] == 'failed':
                reset_table(db_file, 'failed')
            else:
                parser.parse_args(['--help'])

        else:
            parser.parse_args(['--help'])


if __name__ == "__main__":
    main()
