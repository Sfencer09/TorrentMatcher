from io import RawIOBase, BufferedReader, BytesIO
import os
from pprint import pprint
from typing import Any, Dict, List, Tuple

SHA1_HASH_SIZE_BYTES: int = 20

class BEncodeParseError(BaseException):
    def __init__(self, *args):
        super().__init__(*args)

class WrongTorrentFileTypeError(BaseException):
    def __init__(self, *args):
        super().__init__(*args)

def isPowerOf2(n: int) -> bool:
    return (n & (n-1) == 0) and n != 0

class InfoDict:
    isSingleFile: bool
    length: int | None
    files: List[Dict[str, int|List[str]]] | None
    name: str
    piece_length: int
    pieces: List[bytes]
    
    def __init__(self, name: str, piece_length: int, pieces: bytes, *, length: int=None, files: List[Dict[str, int|List[str]]]=None):
        if len(name) == 0:
            raise ValueError("Name cannot be empty")
        # if not isPowerOf2(piece_length):
        #     # raise ValueError("Piece length must be a power of two")
        #     print(f'Encountered unusual piece length: {piece_length} is not a power of two! found in torrent "{name}"')
        # print(len(pieces))
        if len(pieces) % SHA1_HASH_SIZE_BYTES != 0:
            raise ValueError("Piece array byte count not a multiple of 20 (SHA1 hash size)")
        if length is not None:
            if files is not None:
                raise ValueError("Cannot provide both 'length' and 'files'")
            self.isSingleFile = True
            if length <= 0:
                raise ValueError("")
            self.length = length
            self.files = None
        elif files is None:
            raise ValueError("Must provide either 'length' or 'files'")
        else:
            self.isSingleFile = False
            self.files = files
            self.length = None
        self.length = length
        self.name = name
        self.piece_length = piece_length
        self.pieces = [pieces[i:i+SHA1_HASH_SIZE_BYTES] for i in range(0, len(pieces), SHA1_HASH_SIZE_BYTES)]
    
    def __repr__(self) -> str:
        return f"InfoDict(name='{self.name}', piece_length={self.piece_length}, pieces={self.pieces}, {'length='+repr(self.length) if self.length is not None else 'files='+repr(self.files)})"
    
    def getSingleFileHashes(self) -> Dict[str, Tuple[int, bytes]]:
        if self.length is not None:
            #single file torrent
            return {self.name: (0, self.pieces[0])}
        current_position = 0
        # self.piece_length
        file_pieces = {}
        for file in self.files:
            file_length = file['length']
            next_position = current_position + file_length
            first_pieces_index = current_position // self.piece_length
            next_pieces_index = next_position // self.piece_length
            index_diff = next_pieces_index - first_pieces_index
            offset = self.piece_length - (current_position % self.piece_length)
            current_position += file_length
            if file_length < self.piece_length:
                continue
            if offset == self.piece_length:
                offset = 0
            file_path_parts: List[str] = file['path']
            file_full_path = os.path.join(*file_path_parts)
            # print(f"{file_full_path=} {current_position=} {next_position=} {first_pieces_index=} {next_pieces_index=} {self.piece_length=} {index_diff=} {offset=}")
            if offset == 0 and index_diff > 0:
                first_full_file_piece = self.pieces[first_pieces_index]
                # print(first_pieces_index)
                # print(first_full_file_piece)
            elif index_diff > 1:
                first_full_file_piece = self.pieces[first_pieces_index+1]
                # print(first_pieces_index+1)
                # print(first_full_file_piece)
            else:
                continue
            file_pieces[file_full_path] = (offset, first_full_file_piece)
        return file_pieces

    def getAllFileHashes(self) -> Tuple[Dict[str, Tuple[int, bytes]],
                                    List[Tuple[int, bytes, int, List[Tuple[int, str]]]]]:
        if self.length is not None:
            #single file torrent
            return ({self.name: (0, self.pieces[0])}, [])
        current_position = 0
        # self.piece_length
        single_file_pieces = {}
        multi_file_pieces = []
        multi_file_start_piece = None
        multi_file_first_offset = None
        multi_file_hash = None
        multi_files: List[Tuple[int, str]] = []
        for file in self.files:
            file_length = file['length']
            next_position = current_position + file_length
            first_pieces_index = current_position // self.piece_length
            next_pieces_index = next_position // self.piece_length
            index_diff = next_pieces_index - first_pieces_index
            first_piece_offset = self.piece_length - (current_position % self.piece_length)
            
            next_file_offset = self.piece_length - (next_position % self.piece_length)
            current_position += file_length
            if first_piece_offset == self.piece_length:
                first_piece_offset = 0
            if next_file_offset == self.piece_length:
                next_file_offset = 0
            file_path_parts: List[str] = file['path']
            file_full_path = os.path.join(*file_path_parts)
            if multi_file_start_piece is not None:
                multi_files.append((file_length, file_full_path))
                if next_pieces_index != multi_file_start_piece:
                    multi_file_pieces.append((multi_file_first_offset, multi_file_hash, multi_file_start_piece, multi_files))
                    multi_file_start_piece = None
                    multi_file_first_offset = None
                    multi_file_hash = None
                    multi_files = []
            # print(f"{file_full_path=} {current_position=} {next_position=} {first_pieces_index=} {next_pieces_index=} {self.piece_length=} {index_diff=} {offset=}")
            if file_length >= self.piece_length and first_piece_offset == 0 and index_diff > 0:
                first_full_file_piece = self.pieces[first_pieces_index]
                single_file_pieces[file_full_path] = (first_piece_offset, first_full_file_piece)
                # print(first_pieces_index)
                # print(first_full_file_piece)
            elif file_length >= self.piece_length and index_diff > 1:
                first_full_file_piece = self.pieces[first_pieces_index+1]
                single_file_pieces[file_full_path] = (first_piece_offset, first_full_file_piece)
                # print(first_pieces_index+1)
                # print(first_full_file_piece)
            #elif (file_length < self.piece_length and offset == 0) or index_diff > 0:
            #    print(f"{file_length=} {self.piece_length=} {offset=} {index_diff=} {first_pieces_index=} {next_pieces_index=}")
            elif multi_file_start_piece is None:
                multi_file_start_piece = first_pieces_index
                multi_file_first_offset = first_piece_offset
                multi_file_hash = self.pieces[multi_file_start_piece]
                multi_files.append((file_length, file_full_path))
                # elif multi_file_start_piece != first_pieces_index:
                #     ...
            
            if multi_file_start_piece is None and next_file_offset != 0:
                file_length_after_offset = file_length - first_piece_offset
                chunks_skipped = file_length_after_offset // self.piece_length
                file_length_skipped = chunks_skipped * self.piece_length
                file_length_consumed = file_length_skipped + first_piece_offset
                file_length_remaining = file_length - file_length_consumed
                assert file_length_remaining < self.piece_length
                last_piece_offset = self.piece_length - file_length_remaining
                assert next_file_offset + file_length_remaining == self.piece_length
                multi_file_start_piece = first_pieces_index + chunks_skipped
                assert multi_file_start_piece == next_pieces_index - 1
                multi_file_first_offset = last_piece_offset
                multi_file_hash = self.pieces[multi_file_start_piece]
                multi_files.append((file_length, file_full_path))
            
        return (single_file_pieces, multi_file_pieces)
    
class TorrentFile:
    metadata: Dict[str, Any]
    info: InfoDict
    def __init__(self, info: InfoDict, **kwargs ):
        if info is None:
            raise ValueError
        self.metadata = kwargs
        # print(f"{self.metadata=}")
        self.info = info
    
    def __repr__(self) -> str:
        return f"TorrentFile(info={repr(self.info)}, metadata={repr(self.metadata)})"

def parse_torrent(torrent:RawIOBase) -> TorrentFile:
    br = BufferedReader(torrent, 1024*1024)
    def isBnum(b:bytes):
        #print(b, len(b), '\n\n\n')
        assert len(b)==1
        return b[0] >= ord('0') and b[0] <= ord('9')
    def parseString():
        buff = BytesIO()
        b = br.read(1)
        while b != b":":
            buff.write(b)
            b = br.read(1)
        length = int(buff.getvalue().decode())
        bstring = br.read(length)
        #print(br.tell(), bstring)
        return bstring
    def parseInteger():
        constI = br.read(1)
        assert chr(constI[0]) == 'i'
        buff = BytesIO()
        b = br.read(1)
        while b != b"e":
            buff.write(b)
            b = br.read(1)
        ivalue = int(buff.getvalue())
        #print(br.tell(), ivalue)
        return ivalue
    def parseList():
        constL = br.read(1)
        assert chr(constL[0]) == 'l'
        values = []
        b = br.peek(1)[:1]
        while b != b'e':
            # print("[List] Got type ")
            if b == b'i':
                i = parseInteger()
                values.append(i)
            elif b == b'l':
                l = parseList()
                values.append(l)
            elif b == b'd':
                d = parseDictionary()
                values.append(d)
            elif isBnum(b):
                s = parseString()
                try:
                    values.append(s.decode())
                except UnicodeDecodeError:
                    print(f"Unable to decode string {s} as UTF-8")
                    values.append(s)
            else:
                raise BEncodeParseError("Could not parse value " + str(int(b[0])) + " ('" + b.decode() + "') at position " + str(br.tell()))
            b = br.peek(1)[:1]
        br.read(1) #consume the 'e'
        #print(values)
        return values
    def parseDictionary():
        constD = br.read(1)
        assert chr(constD[0]) == 'd'
        assert constD == b'd'
        values = {}
        b1 = br.peek(1)[:1]
        while b1 != b'e':
            #print(b1, '\n\n\n\n')
            if not isBnum(b1):
                raise BEncodeParseError(f"{b1} is not a Bnum")
            # assert isBnum(b1)
            keyBytes = parseString()
            key = keyBytes.decode()
            #print(f"[Dict] Got key '{key}'")
            b2 = br.peek(1)[:1]
            if b2 == b'i':
                value = parseInteger()
            elif b2 == b'l':
                value = parseList()
            elif b2 == b'd':
                value = parseDictionary()
            elif isBnum(b2):
                if key in ('pieces', 'p1', 'info_hash', 'sha1', 'ed2k', 'filehash', 'pieces root'):
                    value = parseString()
                else:
                    temp = parseString()
                    try:
                        value = temp.decode()
                    except:
                        print(f"Unable to decode string from value of key {key}. Value: {temp}")
                        value = temp
            else:
                raise BEncodeParseError("Could not parse value " + str(int(b2[0])) + " ('" + b2.decode() + "') at position " + str(br.tell()))
            values[key] = value
            b1 = br.peek(1)[:1]
        br.read(1) #consume the 'e'
        #print(values)
        return values
    parsedDict = parseDictionary()
    if 'info' not in parsedDict.keys():
        raise WrongTorrentFileTypeError("File was bencoded but did not contain 'info' key")
    #print(parsedDict)
    parsedInfoDict = parsedDict['info']
    #single file torrent
    if 'length' in parsedInfoDict.keys():
        infoDict = InfoDict(parsedInfoDict['name'],
                            parsedInfoDict['piece length'],
                            parsedInfoDict['pieces'],
                            length=parsedInfoDict['length'])
    else:
        infoDict = InfoDict(parsedInfoDict['name'],
                            parsedInfoDict['piece length'],
                            parsedInfoDict['pieces'],
                            files=parsedInfoDict['files'])
    # print(infoDict)
    parsedWithoutInfo = {key:value for key, value in parsedDict.items() if key != 'info'}
    # pprint(parsedWithoutInfo, width=500)
    torrentFile = TorrentFile(infoDict, **parsedWithoutInfo)
    #print(torrentFile)
    return torrentFile
