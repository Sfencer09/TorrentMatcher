from .TorrentFile import parseTorrent
import argparse

argparser = argparse.ArgumentParser("Torrent Matcher", description="A program to find matches between a set of torrent files and a directory of files")

with open("/mnt/pool2/media/rarbg_db.zip.torrent", "rb") as torrentFile:
    tor = parseTorrent(torrentFile)
    #print(tor)

with open("/mnt/pool2/media/Magic the Gathering Novel's - Odyssey Cycle.torrent", "rb") as torrentFile:
    tor = parseTorrent(torrentFile)
    print(repr(tor))
    print(tor.info.getFirstFileHashes())