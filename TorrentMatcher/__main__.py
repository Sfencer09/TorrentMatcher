from .Matcher import match_files
import argparse

argparser = argparse.ArgumentParser("Torrent Matcher", description="A program to find matches between a set of torrent files and a directory of files")

argparser.add_argument('-t', "--torrent", dest='torrentpaths', action="append", help="Path under which to search for torrent files. Can be specified multiple times to search multiple places")
argparser.add_argument('-d', "--downloads", dest='downloadfolders', action="append",  help="Root folder to search for downloaded files. Can be specified multiple times to search multiple places")
argparser.add_argument("--database", default=':memory:', help="Database file to save to, should only be reused with the same torrent file argument. Defaults to :memory:, which does not save after the program finishes")
argparser.add_argument('-j', "--json", dest="jsonpath", default="", help="If specified, writes all found matches to the given JSON file instead of printing to stdout")

args = argparser.parse_args()

print(args)

# with open("/mnt/pool2/media/rarbg_db.zip.torrent", "rb") as torrentFile:
#     tor = parse_torrent(torrentFile)
#     #print(tor)

# with open("/mnt/pool2/media/Magic the Gathering Novel's - Odyssey Cycle.torrent", "rb") as torrentFile:
#     tor = parse_torrent(torrentFile)
#     print(repr(tor))
#     print(tor.info.getFirstFileHashes())

matched_files = match_files(args.torrentpaths, args.downloadfolders, database_path=args.database, json_path=args.jsonpath)