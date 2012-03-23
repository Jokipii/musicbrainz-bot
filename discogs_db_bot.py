from editing import MusicBrainzClient
from discogs_db import DiscogsDbClient
import config as cfg
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A Discogs DB bot', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-l', type=int, help='Set limit of commits. Default 100', default=100)
    parser.add_argument('-a', type=str, required=True,
        choices=('create_functions','create_tables','create_views',
                'do_links','do_release_links','do_artist_links','do_artist_links2',
                'do_release_credits','report','commit_artist_links','commit_artist2_links',
                'commit_artist_types','commit_artist_country','commit_artist_country2',
                'commit_release_links','commit_release_credits'),
        help='''Action
Initial actions:
create_functions            Create functions
create_tables               Create additional tables in Discogs database (release_track_count,
                                country_mapping, release_format) and in MB database 
                                (country_search)
create_views                Create views on MB database (do_artist_link, do_label_link, 
                                do_release_group_link, do_release_link)
Actions after database update:
do_links                    Create link tables (after data updates)
do_release_links            Create release link table (discogs_db_release_link)
do_artist_links             Create artist link table (discogs_db_artist_link)
do_artist_links2            Create VA release track artist link table (discogs_db_artist_link2)
do_release_credits          Create release credits link table (discogs_db_release_artist_credits)
Report actions:
report                      Create stats report in wiki format
Commit actions:
commit_artist_links         Commit artist links to MusicBrainz
commit_artist2_links        Commit VA release track artist links to MusicBrainz
commit_artist_types         Commit artist types based on disambiguation comments
commit_artist_country       Commit artist country based on disambiguation comments
commit_artist_country2      Commit artist country based on Discogs profile text
commit_release_links        Commit release links to MusicBrainz
commit_release_credits      Commit release artist relationships to MusicBrainz''')
    args = parser.parse_args()
    if args.a:
        doClient = DiscogsDbClient()
        if args.a == 'create_functions':
            doClient.createfunctions()
            print "functions done!"
        elif args.a == 'create_tables':
            doClient.create_country_mapping_table()
            doClient.create_country_search_table()
            doClient.create_media_type_mapping_table()
            doClient.create_track_count()
            print "create tables done!"
        elif args.a == 'create_views':
            doClient.create_link_views()
            print "create tables done!"
        elif args.a == 'do_links':
            doClient.createlinks()
            print "links done!"
        elif args.a == 'do_release_links':
            doClient.release_link_table()
            print "release links done!"
        elif args.a == 'do_artist_links':
            doClient.artist_link_table()
            print "artist links done!"
        elif args.a == 'do_artist_links2':
            doClient.artist_link_table()
            print "artist links done!"
        elif args.a == 'report':
            doClient.report()
        elif args.a == 'do_release_credits':
            doClient.release_artist_link_table()
        elif args.a == 'commit_artist_links':
            doClient.commit_artist_links(args.l)
        elif args.a == 'commit_artist2_links':
            doClient.commit_artist_links2(args.l)
        elif args.a == 'commit_artist_types':
            doClient.commit_artist_types(args.l)
        elif args.a == 'commit_artist_country':
            doClient.commit_artist_country(args.l)
        elif args.a == 'commit_artist_country2':
            doClient.commit_artist_country2(args.l)
        elif args.a == 'commit_release_links':
            doClient.commit_release_links(args.l)
        elif args.a == 'commit_release_credits':
            doClient.commit_release_artist_relationship(args.l)
    else:
        parser.print_help()
