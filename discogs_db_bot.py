from editing import MusicBrainzClient
from discogs_db import DiscogsDbClient
import config as cfg
import argparse

doClient = DiscogsDbClient()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A Discogs DB bot')
    parser.add_argument('-a', action='store_true', help='Create artist link table (discogs_db_artist_link)')
    parser.add_argument('-al', type=int, help='Commit artist links to MusicBrainz, where AL is max number of commits')
    parser.add_argument('-f', action='store_true', help='Discogs additional tables (release_track_count)')
    parser.add_argument('-f', action='store_true', help='Create functions')
    parser.add_argument('-l', action='store_true', help='Create link tables (after data updates)')
    parser.add_argument('-r', action='store_true', help='Create release link table (discogs_db_release_link)')
    parser.add_argument('-rl', type=int, help='Commit release links to MusicBrainz, where RL is max number of commits')
    args = parser.parse_args()
    if (args.a):
        doClient.artist_link_table()
        print "artist links table done!"
    elif (args.al):
        doClient.commit_artist_links(args.al)
    elif (args.d):
        doClient.create_track_count()
        print "Discogs additional tables done!"
    elif (args.f):
        doClient.createfunctions()
        print "functions done!"
    elif (args.l):
        doClient.createlinks()
        print "links done!"
    elif (args.r):
        doClient.release_link_table()
        print "release links table done!"
    elif (args.rl):
        doClient.commit_release_links(args.rl)
    else:
        parser.print_help()