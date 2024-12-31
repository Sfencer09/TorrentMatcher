from .Matcher import match_files
import argparse

argparser = argparse.ArgumentParser("Torrent Matcher", description="A program to find matches between a set of torrent files and a directory of files")

argparser.add_argument("torrentpath", help="Path under which to search for torrent files")
argparser.add_argument("downloadfolder", help="Root folder to search for downloaded files")
argparser.add_argument("database", nargs='?', default=':memory:', help="Database file to save to, should only be reused with the same torrent file argument. Defaults to :memory:, which does not save after the program finishes")

args = argparser.parse_args()

print(args)

# with open("/mnt/pool2/media/rarbg_db.zip.torrent", "rb") as torrentFile:
#     tor = parse_torrent(torrentFile)
#     #print(tor)

# with open("/mnt/pool2/media/Magic the Gathering Novel's - Odyssey Cycle.torrent", "rb") as torrentFile:
#     tor = parse_torrent(torrentFile)
#     print(repr(tor))
#     print(tor.info.getFirstFileHashes())

matched_files = match_files(args.torrentpath, args.downloadfolder, database_path=args.database)