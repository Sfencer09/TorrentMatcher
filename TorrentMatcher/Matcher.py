import json
import os
import sys
import sqlite3
import hashlib
from traceback import print_exc, print_exception
from typing import List, Tuple
# import psutil

from .DownloadedFile import DownloadedFile

from .TorrentFile import BEncodeParseError, TorrentFile, WrongTorrentFileTypeError, parse_torrent

# TORRENT_HASH_TABLE = "torrentFirstHashes"
# DOWNLOADED_FILE_TABLE = "downloadedFiles"
# DOWNLOADED_HASH_TABLE = "downloadedFirstHashes"

def setup_database(conn: sqlite3.Connection):
    cur = conn.cursor()
    """,
        FOREIGN KEY torrentFileRowId REFERENCES torrentFile(ROWID),
        PRIMARY KEY torrentFileRowId,
        PRIMARY KEY fileName"""
    """,
        FOREIGN KEY(fileRowId) REFERENCES downloadedFile(ROWID),
        PRIMARY KEY fileRowId,
        PRIMARY KEY filePath"""
    cur.executescript("""
    BEGIN;
    CREATE TABLE IF NOT EXISTS torrentFile (
        torrentPath text,
        torrentName text NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS torrentFirstHashes (
        torrentFileRowId INTEGER NOT NULL,
        fileName text NOT NULL,
        pieceSize INTEGER NOT NULL,
        fileSize INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        hash BLOB NOT NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_torrentFirstHashes_size ON torrentFirstHashes (fileSize);
    
    CREATE TABLE IF NOT EXISTS downloadedFile (
        filePath text NOT NULL,
        fileSize INTEGER NOT NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_downloadedFile ON downloadedFile (fileSize);
    
    CREATE TABLE IF NOT EXISTS downloadedFirstHashes (
        fileRowId INTEGER NOT NULL,
        filePath text NOT NULL,
        pieceSize INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        hash BLOB NOT NULL
    );
    COMMIT;
""")
    conn.commit()
    

def match_files(torrent_files_paths: List[str], file_search_paths: List[str], *, database_path: str=':memory:', json_path: str=None) -> List[Tuple[DownloadedFile, TorrentFile]]:
    write_json = json_path is not None and len(json_path) > 0
    if not all((os.path.exists(torrent_files_path) for torrent_files_path in torrent_files_paths)):
        raise ValueError("Torrent file path does not exist")
    if not all((os.path.exists(file_search_path) for file_search_path in file_search_paths)):
        raise ValueError("Path to downloaded files does not exist")
    if not all((os.path.isdir(file_search_path) for file_search_path in file_search_paths)):
        raise ValueError("Path to downloaded files must point to a folder")
    conn = sqlite3.connect(database_path)
    setup_database(conn)
    
    
    def save_torrent_file(torrent_file: TorrentFile, file_path: str):
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO torrentFile(torrentPath, torrentName) VALUES(?, ?)", (file_path, torrent_file.info.name))
        conn.commit()
        torrent_file_row_id = cur.execute("SELECT ROWID FROM torrentFile WHERE torrentPath = ?", (file_path, )).fetchone()[0]
        # print(f"{torrent_file_row_id=}")
        first_hashes = torrent_file.info.getFirstFileHashes()
        piece_length = torrent_file.info.piece_length
        if torrent_file.info.isSingleFile:
            file_size = torrent_file.info.length
            assert len(first_hashes) == 1
            filename = list(first_hashes.keys())[0]
            # print(f"{filename=}")
            # print(f"{first_hashes=}")
            offset, first_hash = first_hashes[filename]
            cur.execute("INSERT OR IGNORE INTO torrentFirstHashes(torrentFileRowId, fileName, pieceSize, fileSize, offset, hash) VALUES(?, ?, ?, ?, ?, ?)", 
                        (torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
            # print((torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
        else:
            for filename in first_hashes.keys():
                offset, first_hash = first_hashes[filename]
                file_size = [file for file in torrent_file.info.files if os.path.join(*file['path']) == filename][0]['length']
                # print((torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
                cur.execute("INSERT OR IGNORE INTO torrentFirstHashes(torrentFileRowId, fileName, pieceSize, fileSize, offset, hash) VALUES(?, ?, ?, ?, ?, ?)", 
                            (torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
        conn.commit()
    
    torrent_files: List[TorrentFile] = []
    
    # print(psutil.virtual_memory())
    for torrent_files_path in torrent_files_paths:
        if os.path.isfile(torrent_files_path):
            assert torrent_files_path.endswith(".torrent")
            with open(torrent_files_path, "rb") as torrent_file_data:
                torrent_file = parse_torrent(torrent_file_data)
                save_torrent_file(torrent_file, torrent_files_path)
                torrent_files.append(torrent_file)
        else:
            for root, _, files in os.walk(torrent_files_path):
                for file in (os.path.join(root, file) for file in files):
                    if file.endswith(".torrent"):
                        try:
                            with open(file, "rb") as torrent_file_data:
                                torrent_file = parse_torrent(torrent_file_data)
                                save_torrent_file(torrent_file, file)
                                torrent_files.append(torrent_file)
                        except WrongTorrentFileTypeError as wtfte:
                            print(wtfte, f"File path: {file}", sep="\n    ")
                        # except UnicodeDecodeError as ude:
                        #     print(ude, f"File path: {file}", sep="\n    ")
                        except BEncodeParseError as bepe:
                            print(bepe, f"File path: {file}", sep="\n    ")
        cur = conn.cursor()
    if json_path is None or len(json_path) == 0:
        print(f"Max piece length= {max((tf.info.piece_length for tf in torrent_files))}")
        print(f"Min piece length= {min((tf.info.piece_length for tf in torrent_files))}")
    print(f"Number of torrent file entries: {len(torrent_files)}")
    
    print("Finished reading torrent files, starting initial fast scan of downloaded files")
    
    # print(psutil.virtual_memory())
    # downloaded_files: List[DownloadedFile] = []
    for file_search_path in file_search_paths:
        for root, _, files in os.walk(file_search_path):
            for file in (os.path.join(root, file) for file in files):
                file_size = os.path.getsize(file)
                cur.execute("INSERT OR IGNORE INTO downloadedFile(filePath, fileSize) VALUES (?, ?)", (file, file_size))
                # downloaded_files.append(DownloadedFile(file, file_size))
        conn.commit()
    
    # TODO: fetch torrent info as well to more quickly match to full torrent path, & do it in application rather than in db
    cur.execute("SELECT downloadedFile.ROWID, downloadedFile.filePath, pieceSize, offset, hash FROM downloadedFile INNER JOIN torrentFirstHashes ON downloadedFile.fileSize = torrentFirstHashes.fileSize ORDER BY downloadedFile.filePath")
    
    print("Fast scan finished, beginning deep scan")
    queued_file = None
    queued_file_rowId = None
    # queued_df = None
    queued_records: List[Tuple[int, int, bytes]] = None
    
    def processQueue():
        assert queued_file is not None
        # start = min((record[1] for record in queued_records))
        end = max((record[0]+record[1] for record in queued_records)) #noninclusive
        # queued_records.sort()
        with open(queued_file, 'rb') as testing_file:
            testing_data = testing_file.read(end)
            if len(testing_data) != end:
                print(f"Unable to read full part of file {queued_file}")
                return
        assert len(testing_data) == end
        # last_piece_size = None
        # last_offset = None
        for piece_size, offset, search_hash in queued_records:
            # if piece_size == last_piece_size and offset == last_offset:
            section = testing_data[offset:offset+piece_size]
            calculated_hash = hashlib.sha1(section, usedforsecurity=False).digest()
            # if calculated_hash == search_hash:
            #     print(f"Found hash match! File: {queued_file}")
            cur.execute("INSERT OR IGNORE INTO downloadedFirstHashes(fileRowId, filePath, pieceSize, offset, hash) VALUES (?, ?, ?, ?, ?)", (queued_file_rowId, queued_file, piece_size, offset, calculated_hash))
            
    
    for ROWID, filePath, piece_size, offset, found_hash in cur.fetchall():
        if filePath != queued_file:
            if queued_file is not None:
                processQueue()
                # conn.commit()
            queued_file = filePath
            # queued_df: DownloadedFile = filter(lambda x: x.path == filePath, downloaded_files)[0]
            queued_file_rowId = ROWID
            queued_records = []
        queued_records.append((piece_size, offset, found_hash))
        # queued_df.add_hash(piece_size, offset, found_hash)
    processQueue()
    conn.commit()
    
    print("Deep scan completed, beginning matching process")
    
    successful_matches = cur.execute("""SELECT downloadedFile.filePath, torrentFile.torrentPath, torrentFirstHashes.fileName
                                     FROM downloadedFirstHashes
                                     INNER JOIN downloadedFile ON downloadedFile.ROWID = downloadedFirstHashes.fileRowId
                                     INNER JOIN torrentFirstHashes ON downloadedFile.fileSize = torrentFirstHashes.fileSize
                                     AND torrentFirstHashes.pieceSize = downloadedFirstHashes.pieceSize 
                                     AND torrentFirstHashes.offset = downloadedFirstHashes.offset
                                     AND torrentFirstHashes.hash = downloadedFirstHashes.hash
                                     INNER JOIN torrentFile ON torrentFile.ROWID = torrentFirstHashes.torrentFileRowId""").fetchall()
    
    print(f"Matching process complete! Found {len(successful_matches)} matches:")
    matches_json = {}
    for downloaded_path, torrent_file_path, torrent_sub_path in successful_matches:
        if write_json:
            if torrent_file_path not in matches_json.keys():
                matches_json[torrent_file_path] = {}
            if torrent_sub_path in matches_json[torrent_file_path].keys():
                if downloaded_path not in matches_json[torrent_file_path][torrent_sub_path]:
                    matches_json[torrent_file_path][torrent_sub_path].append(downloaded_path)
            else:
                matches_json[torrent_file_path][torrent_sub_path] = [downloaded_path]
        else:
            print(f"File on disk: {downloaded_path}")
            print(f"Torrent file: {torrent_file_path}")
            print(f"Path within torrent: {torrent_sub_path}")
            print()
    if write_json:
        with open(json_path, 'w') as json_file:
            json.dump(matches_json, json_file, indent=4)