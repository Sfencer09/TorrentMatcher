import json
import os
import sys
import sqlite3
import hashlib
from traceback import print_exc, print_exception
from typing import Dict, List, Tuple
# import psutil

from .DownloadedFile import DownloadedFile

from .TorrentFile import BEncodeParseError, TorrentFile, WrongTorrentFileTypeError, parse_torrent

# TORRENT_HASH_TABLE = "torrentSingleFileHash"
# DOWNLOADED_FILE_TABLE = "downloadedFile"
# DOWNLOADED_HASH_TABLE = "downloadedFirstHash"

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
        torrentPath TEXT,
        torrentName TEXT NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS torrentSingleFileHash (
        torrentFileRowId INTEGER NOT NULL,
        fileName TEXT NOT NULL,
        pieceSize INTEGER NOT NULL,
        fileSize INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        hash BLOB NOT NULL,
        PRIMARY KEY (torrentFileRowId, fileName)
    );
    
    CREATE INDEX IF NOT EXISTS idx_torrentSingleFileHash_size ON torrentSingleFileHash (fileSize);
    
    CREATE TABLE IF NOT EXISTS torrentMultiFileHash (
        torrentFileRowId INTEGER NOT NULL,
        pieceIndex INTEGER NOT NULL,
        pieceSize INTEGER NOT NULL,
        firstFileOffset INTEGER NOT NULL,
        hash BLOB NOT NULL,
        PRIMARY KEY (torrentFileRowId, pieceIndex)
    );
    
    CREATE TABLE IF NOT EXISTS torrentMultiFileHashFile (
        multiFileHashRowId INTEGER PRIMARY KEY NOT NULL,
        fileOrder INTEGER NOT NULL,
        fileSize INTEGER NOT NULL,
        fileName TEXT NOT NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_torrentMultiFileHashFile_size ON torrentMultiFileHashFile (fileSize);
    
    CREATE TABLE IF NOT EXISTS torrentMultiFileHashFileMatch (
        multiFileHashFileRowId INTEGER NOT NULL,
        downloadedFileRowId INTEGER NOT NULL,
    )
    
    CREATE TABLE IF NOT EXISTS downloadedFile (
        filePath TEXT NOT NULL,
        fileSize INTEGER NOT NULL
    );
    
    CREATE INDEX IF NOT EXISTS idx_downloadedFile ON downloadedFile (fileSize);
    
    CREATE INDEX IF NOT EXISTS idx_downloadedFile ON downloadedFile (filePath);
    
    CREATE TABLE IF NOT EXISTS downloadedFirstHash (
        fileRowId INTEGER NOT NULL,
        filePath TEXT NOT NULL,
        pieceSize INTEGER NOT NULL,
        offset INTEGER NOT NULL,
        hash BLOB NOT NULL,
        PRIMARY KEY (fileRowId, filePath)
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
        single_file_hashes = torrent_file.info.getSingleFileHash()
        single_file_hashes2, multi_file_hashes = torrent_file.info.getAllFileHash()
        assert single_file_hashes2 == single_file_hashes
        print(f"{multi_file_hashes=}")
        piece_length = torrent_file.info.piece_length
        if torrent_file.info.isSingleFile:
            file_size = torrent_file.info.length
            assert len(single_file_hashes) == 1
            filename = list(single_file_hashes.keys())[0]
            # print(f"{filename=}")
            # print(f"{first_hashes=}")
            offset, first_hash = single_file_hashes[filename]
            cur.execute("INSERT OR IGNORE INTO torrentSingleFileHash(torrentFileRowId, fileName, pieceSize, fileSize, offset, hash) VALUES(?, ?, ?, ?, ?, ?)", 
                        (torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
            # print((torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
        else:
            for filename in single_file_hashes.keys():
                offset, first_hash = single_file_hashes[filename]
                file_size = [file for file in torrent_file.info.files if os.path.join(*file['path']) == filename][0]['length']
                # print((torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
                cur.execute("INSERT OR IGNORE INTO torrentSingleFileHash(torrentFileRowId, fileName, pieceSize, fileSize, offset, hash) VALUES(?, ?, ?, ?, ?, ?)", 
                            (torrent_file_row_id, filename, piece_length, file_size, offset, first_hash))
            for first_offset, hash, piece_index, files_info in multi_file_hashes:
                cur.execute("INSERT OR IGNORE INTO torrentMultiFileHash(torrentFileRowId, pieceIndex, pieceSize, firstFileOffset, hash) VALUES(?, ?, ?, ?, ?)",
                            (torrent_file_row_id, piece_index, piece_length, first_offset, hash))
                conn.commit()
                cur.execute("SELECT ROWID FROM torrentMultiFileHash WHERE torrentFileRowId = ? AND pieceIndex = ?",
                            (torrent_file_row_id, piece_index))
                multi_file_hash_row_id = cur.fetchone()[0]
                cur.executemany("INSERT OR IGNORE INTO torrentMultiFileHashFile(multiFileHashRowId, fileOrder, fileSize, fileName) VALUES(?, ?, ?, ?)",
                            ((multi_file_hash_row_id, file_index, file_size, file_path) for file_index, (file_size, file_path) in enumerate(files_info)))
                conn.commit()
                    
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
    cur.execute("""SELECT downloadedFile.ROWID, downloadedFile.filePath, pieceSize, offset, hash 
                FROM downloadedFile 
                INNER JOIN torrentSingleFileHash 
                    ON downloadedFile.fileSize = torrentSingleFileHash.fileSize 
                ORDER BY downloadedFile.filePath""")
    
    print("Fast scan finished, beginning deep scan")
    queued_file = None
    queued_file_rowId = None
    # queued_df = None
    queued_single_records: List[Tuple[int, int, bytes]] = None
    
    def processSingleFileQueue():
        assert queued_file is not None
        # start = min((record[1] for record in queued_records))
        read_end = max((record[0]+record[1] for record in queued_single_records)) #noninclusive
        # queued_records.sort()
        with open(queued_file, 'rb') as testing_file:
            testing_data = testing_file.read(read_end)
            if len(testing_data) != read_end:
                print(f"Unable to read full part of file {queued_file}")
                return
        # assert len(testing_data) == read_end
        # last_piece_size = None
        # last_offset = None
        for piece_size, offset, search_hash in queued_single_records:
            # if piece_size == last_piece_size and offset == last_offset:
            section = testing_data[offset:offset+piece_size]
            calculated_hash = hashlib.sha1(section, usedforsecurity=False).digest()
            # if calculated_hash == search_hash:
            #     print(f"Found hash match! File: {queued_file}")
            cur.execute("INSERT OR IGNORE INTO downloadedFirstHash(fileRowId, filePath, pieceSize, offset, hash) VALUES (?, ?, ?, ?, ?)", 
                        (queued_file_rowId, queued_file, piece_size, offset, calculated_hash))

        
    
    for ROWID, filePath, piece_size, offset, found_hash in cur.fetchall():
        if filePath != queued_file:
            if queued_file is not None:
                processSingleFileQueue()
                # conn.commit()
            queued_file = filePath
            # queued_df: DownloadedFile = filter(lambda x: x.path == filePath, downloaded_files)[0]
            queued_file_rowId = ROWID
            queued_single_records = []
        queued_single_records.append((piece_size, offset, found_hash))
        # queued_df.add_hash(piece_size, offset, found_hash)
    processSingleFileQueue()
    conn.commit()


    #################################
    ####### Multi-file hashes #######
    #################################
    
    cur.execute("""
                SELECT downloadedFile.filePath,
                       downloadedFile.fileSize
                FROM downloadedFile 
                INNER JOIN torrentMultiFileHashFile 
                    ON downloadedFile.fileSize=torrentMultiFileHashFile.fileSize;
                """)
    
    filePathsBySize: Dict[int, List[str]] = {}
    for filePath, fileSize in cur:
        try:
            filePathsBySize[fileSize].append(filePath)
        except:
            filePathsBySize[fileSize] = [filePath]
    

    cur.execute("""SELECT torrentMultiFileHash.ROWID,
                          torrentMultiFileHash.hash,
                          torrentMultiFileHash.pieceSize,
                          torrentMultiFileHash.firstFileOffset,
                          torrentMultiFileHashFile.ROWID,
                          torrentMultiFileHashFile.fileOrder,
                          torrentMultiFileHashFile.fileSize,
                    FROM torrentMultiFileHash
                    INNER JOIN torrentMultiFileHashFile
                        ON torrentMultiFileHash.ROWID=torrentMultiFileHashFile.multiFileHashRowId
                    ORDER BY torrentMultiFileHash.ROWID, torrentMultiFileHashFile.fileOrder;
                          """)
    
    current_multi_file_hash_row_id = None
    currentMultiFileHashHash = None
    currentMultiFileHashPieceSize = None
    currentMultiFileHashFirstOffset = None
    currentMultiFileHashFileInfos = None
    
    def searchFiles(fileSizeList: List[Tuple[int, int]], remainingLength: int, offset=0, digest: hashlib._Hash | None=None, hopefulPairs: List[Tuple[int, int]]=None):
        assert remainingLength > 0 and remainingLength < currentMultiFileHashPieceSize
        assert (offset != 0 and digest is None and len(fileSizeList) > 1) or (len(fileSizeList) >= 1 and digest is not None and offset == 0)
        currentFileSize, multiFileHashFileRowId = fileSizeList[0]
        remainingFileSizes = fileSizeList[1:]
        readLength = min(currentFileSize-offset, remainingLength)
        if hopefulPairs is None:
            hopefulPairs = []
        if digest is None:
            digest = hashlib.sha1(usedforsecurity=False)
        
        for filePath in filePathsBySize[currentFileSize]:
            digestCopy = digest.copy()
            with open(filePath, 'rb') as testFile:
                if offset != 0:
                    testFile.seek(offset)
                testFileData = testFile.read(readLength)
                if not len(testFileData) == readLength:
                    print(f"Error reading {filePath}!")
                    continue
                digestCopy.update(testFileData)
            cur.execute("SELECT ROWID FROM downloadedFile WHERE filePath = ?", (filePath, ))
            downloadedFileRowId = cur.fetchone()[0]
            hopefulPairs.append((multiFileHashFileRowId, downloadedFileRowId))
            if len(remainingFileSizes) == 0:
                # last file, test against hash
                completeHash = digest.digest()
                if completeHash == currentMultiFileHashHash:
                    print("Found successful multi-file match!")
                    for mfhfri, dfri in hopefulPairs:
                        cur.execute("INSERT INTO torrentMultiFileHashFileMatch(multiFileHashFileRowId, downloadedFileRowId) VALUES(?, ?)", 
                                (mfhfri, dfri))
            else:
                newRemainingLength = remainingLength - currentFileSize
                searchFiles(remainingFileSizes, newRemainingLength, digest=digestCopy, hopefulPairs=hopefulPairs)
    
    lastFileOrder = None
    
    for multiFileHashRowId, combinedHash, pieceSize, firstOffset, multiFileHashFileRowId, fileOrder, fileSize in cur:
        if current_multi_file_hash_row_id is not None and current_multi_file_hash_row_id != multiFileHashRowId:
            searchFiles(currentMultiFileHashFileInfos, currentMultiFileHashPieceSize, currentMultiFileHashFirstOffset)
        
        if current_multi_file_hash_row_id is None or current_multi_file_hash_row_id != multiFileHashRowId:
            current_multi_file_hash_row_id = multiFileHashRowId
            currentMultiFileHashHash = combinedHash
            currentMultiFileHashPieceSize = pieceSize
            currentMultiFileHashFirstOffset = firstOffset
            currentMultiFileHashFileInfos = []
        if lastFileOrder is not None:
            if fileOrder != 0 and fileOrder != lastFileOrder + 1:
                print(f"Out of order file found! last file number: {lastFileOrder} current file number: {fileOrder}")
        lastFileOrder = fileOrder
        currentMultiFileHashFileInfos.append((fileSize, multiFileHashFileRowId))

    
    print("Deep scan completed, beginning matching process")
    
    successful_single_file_matches = cur.execute("""SELECT downloadedFile.filePath, torrentFile.torrentPath, torrentSingleFileHash.fileName
                                     FROM downloadedFirstHash
                                     INNER JOIN downloadedFile ON downloadedFile.ROWID = downloadedFirstHash.fileRowId
                                     INNER JOIN torrentSingleFileHash ON downloadedFile.fileSize = torrentSingleFileHash.fileSize
                                     AND torrentSingleFileHash.pieceSize = downloadedFirstHash.pieceSize 
                                     AND torrentSingleFileHash.offset = downloadedFirstHash.offset
                                     AND torrentSingleFileHash.hash = downloadedFirstHash.hash
                                     INNER JOIN torrentFile ON torrentFile.ROWID = torrentSingleFileHash.torrentFileRowId""").fetchall()
    
    print(f"Matching process complete! Found {len(successful_single_file_matches)} matches:")
    matches_json = {}
    for downloaded_path, torrent_file_path, torrent_sub_path in successful_single_file_matches:
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
    
    
    successful_multi_file_matches = cur.execute("""SELECT downloadedFile.filePath, torrentFile.torrentPath, torrentMultiFileHashFile.fileName
                                                FROM downloadedFile
                                                INNER JOIN torrentMultiFileHashFileMatch ON torrentMultiFileHashFileMatch.downloadedFileRowId=downloadedFile.ROWID
                                                INNER JOIN torrentMultiFileHashFile ON torrentMultiFileHashFileMatch.multiFileHashFileRowId=torrentMultiFileHashFile.ROWID
                                                INNER JOIN torrentMultiFileHash ON torrentMultiFileHashFile.multiFileHashRowId=torrentMultiFileHash.ROWID
                                                INNER JOIN torrentFile ON torrentMultiFileHash.torrentFileRowId=torrentFile.ROWID""")
    
    for downloaded_path, torrent_file_path, torrent_sub_path in successful_multi_file_matches:
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