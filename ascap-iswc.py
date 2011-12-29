from editing import MusicBrainzClient
from ascap import AscapClient
import config as cfg
import argparse

aClient = AscapClient()

def fetch_artist(name):
    ids, names, ipicodes = aClient.search_writer(name);
    for i in range(len(ids)):
        print names[i] + '\t' + ipicodes[i]+ '\n'
        aClient.get_artist_works(ids[i])
        aClient.do_works()

def push_data():
    mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
    for row in aClient.get_mb_data():
        gid, workid, iswc, artist, work = row
        note = 'Data taken from ' + aClient.search_url(str(workid), 'i')
        mb.edit_work(gid, {'iswc': iswc}, note)
        print artist + ' work: ' + work + ' Done!'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A bot that get IPI and ISWC codes from ASCAP web site')
    parser.add_argument('-a', help='Give artist name which information should be fetched from ASCAP web site')
    parser.add_argument('-i', action='store_true', help='Collect missing IPI codes from ASCAP web site')
    parser.add_argument('-p', action='store_true', help='Push data to MusicBrainz')
    args = parser.parse_args()
    if (args.a != None):
        fetch_artist(args.a)
    if (args.i):
        aClient.do_ipis()
    if (args.p):
        push_data()
    if (args.a == None and not args.i and not args.p):
        parser.print_help()