from editing import MusicBrainzClient
from discogs_db import DiscogsDbClient
import config as cfg
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A Discogs DB bot', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-l', type=int, help='Set limit of commits. Default 100', default=100)
    parser.add_argument('-a', type=str, required=True,
        choices=('init_functions','init_tables','init_views',
                'clean_release_identifiers', 'clean_artist_identifiers',
                'do_links', 'do_artist_evidence_track', 'do_artist_all',
                'do_release_links',
                'do_artist_links', 'do_artist_links2',
                'do_artist_links3', 'do_artist_links4', 'do_artist_links5',
                'do_artist_links6', 'do_artist_links7',
                'do_member_of_band', 'do_perform_as',
                'do_release_credits','do_release_format','do_release_barcode',
                'do_label_links',
                'do_recording_credits',
                'report',
                'commit_artist_links','commit_artist_links2','commit_artist_links3',
                'commit_artist_links4', 'commit_artist_links5', 'commit_artist_links6',
                'commit_artist_links7', 
                'commit_artist_all', 'commit_artist_all2',
                'commit_member_of_band', 'commit_perform_as',
                'commit_label_links',
                'commit_release_links','commit_release_credits', 'commit_release_format',
                'commit_release_barcode',
                'commit_recording_credits', 
                'run_artist_404', 'run_label_404',
                'run_artist_types','run_artist_country','run_artist_country2'
                ),
        help='''Action
Initial actions:
init_functions              Create functions
init_tables                 Create additional tables in Discogs database (release_track_count,
                                country_mapping, release_format) and in MB database 
                                (country_search)
init_views                  Create views on MB database (do_artist_link, do_label_link, 
                                do_release_group_link, do_release_link)
clean_release_identifiers   Cleans release_identifiers
clean_artist_identifiers    Cleans artist_id fields and creates missing foreing keys

Actions after database update (create tables for commit actions):
do_links                    Create link tables (after data updates)
do_artist_evidence_track    Create artist evidence table based on release track artists
                                (discogs_db_artist_evidence_track)
do_artist_all               Create artist evidence table with all evidence
                                (discogs_db_artist_all)
do_artist_links             Create artist link table (discogs_db_artist_link)
do_artist_links2            Create VA release track artist link table (discogs_db_artist_link2)
do_artist_links3            Create artist link table based on band membership 
                                (discogs_db_artist_link3)
do_artist_links4            Create artist link table based on external links
                                (discogs_db_artist_link4)
do_artist_links5            Create artist link table based on band members
                                (discogs_db_artist_link5)
do_artist_links6            Create artist link table based on realname
                                (discogs_db_artist_link6)
do_artist_links7            Create artist link table based on release names
                                (discogs_db_artist_link7)
do_member_of_band           Create member of band relations based on Discogs
                                (discogs_db_member_of_band)
do_perform_as               Create perform as relations based on Discogs
                                (discogs_db_perform_as)
do_label_links              Create label link table (discogs_db_label_link)
do_release_links            Create release link table (discogs_db_release_link)
do_release_credits          Create release credits link table 
                                (discogs_db_release_artist_credits)
do_release_format           Create release format table (discogs_db_release_format)
do_release_barcode          Create release barcode table (discogs_db_release_barcode)
do_recording_credits        Create remixer table (discogs_db_recording_credits)

Report actions:
report                      Create stats report in wiki format

Commit actions (statefull actions, dependent on do_* actions):
commit_artist_all           Commit artist links based on name and other evidence
commit_artist_all2          Commit artist links based on sort name and other evidence
commit_artist_links         Commit artist links
commit_artist_links2        Commit VA release track artist links
commit_artist_links3        Commit artist links based on band membership
commit_artist_links4        Commit artist links based on name and external urls
commit_artist_links5        Commit artist links based on already linked band member
commit_artist_links6        Commit artist links based on realname/performer name
commit_artist_links7        Commit artist links based on release names
commit_member_of_band       Commit member of band links based on Discogs data
commit_perform_as           Commit perform as links based on Discogs data
commit_label_links          Commit label links
commit_release_links        Commit release links
commit_release_credits      Commit release advanced relationships
commit_release_format       Commit release format based on Discogs release
commit_release_barcode      Commit release barcode based on Discogs release
commit_recording_credits    Commit remixer based on Discogs track artist credits

Run actions (stateless actions, dependend on MB database updates):
run_artist_404              Commit removal of 404 links
run_label_404               Commit removal of 404 links
run_artist_types            Commit artist types based on disambiguation comments
run_artist_country          Commit artist country based on disambiguation comments
run_artist_country2         Commit artist country based on Discogs profile text
''')
    args = parser.parse_args()
    if args.a:
        doClient = DiscogsDbClient()
        if args.a == 'create_functions':
            doClient.create_functions()
            print "functions done!"
        elif args.a == 'create_tables':
            doClient.create_country_mapping_table()
            doClient.create_country_search_table()
            doClient.create_media_type_mapping_table()
            doClient.create_release_status_table()
            print "country, media_type and release_status tables done!"
            doClient.create_track_count()
            print "track count done!"
            doClient.create_release_mb_mapping_table()
            print "create tables done!"
        elif args.a == 'create_views':
            doClient.create_link_views()
            print "create tables done!"
        elif args.a == 'clean_release_identifiers':
            doClient.clean_release_identifiers()
        elif args.a == 'clean_artist_identifiers':
            doClient.clean_artist_identifiers()

        elif args.a == 'do_links':
            doClient.do_links()
            print "links done!"

        elif args.a == 'do_artist_evidence_track':
            doClient.do_artist_evidence_track()
        elif args.a == 'do_artist_all':
            doClient.do_artist_all()
        elif args.a == 'do_artist_links':
            doClient.do_artist_link_table()
            print "artist links done!"
        elif args.a == 'do_artist_links2':
            doClient.do_artist_link_table2()
            print "artist links done!"
        elif args.a == 'do_artist_links3':
            doClient.do_artist_link_table3()
            print "artist links done!"
        elif args.a == 'do_artist_links4':
            doClient.do_artist_link_table4()
            print "artist links done!"
        elif args.a == 'do_artist_links5':
            doClient.do_artist_link_table5()
            print "artist links done!"
        elif args.a == 'do_artist_links6':
            doClient.do_artist_link_table6()
            print "artist links done!"
        elif args.a == 'do_artist_links7':
            doClient.do_artist_link_table7()
            print "artist links done!"
        elif args.a == 'do_member_of_band':
            doClient.do_member_of_band_table()
        elif args.a == 'do_perform_as':
            doClient.do_perform_as_table()

        elif args.a == 'do_label_links':
            doClient.do_label_link_table()
            print "label links done!"

        elif args.a == 'do_release_links':
            doClient.do_release_link_table()
            print "release links done!"
        elif args.a == 'do_release_credits':
            doClient.do_release_credits_table()
        elif args.a == 'do_release_format':
            doClient.do_release_format_table()
        elif args.a == 'do_release_barcode':
            doClient.do_release_barcode_table()

        elif args.a == 'do_recording_credits':
            doClient.do_recording_credits_table()

        elif args.a == 'report':
            doClient.report()

        elif args.a == 'commit_artist_all':
            doClient.commit_artist_all(args.l)
        elif args.a == 'commit_artist_all2':
            doClient.commit_artist_all2(args.l)
        elif args.a == 'commit_artist_links':
            doClient.commit_artist_links(args.l)
        elif args.a == 'commit_artist_links2':
            doClient.commit_artist_links2(args.l)
        elif args.a == 'commit_artist_links3':
            doClient.commit_artist_links3(args.l)
        elif args.a == 'commit_artist_links4':
            doClient.commit_artist_links4(args.l)
        elif args.a == 'commit_artist_links5':
            doClient.commit_artist_links5(args.l)
        elif args.a == 'commit_artist_links6':
            doClient.commit_artist_links6(args.l)
        elif args.a == 'commit_artist_links7':
            doClient.commit_artist_links7(args.l)
        elif args.a == 'commit_member_of_band':
            doClient.commit_member_of_band(args.l)
        elif args.a == 'commit_perform_as':
            doClient.commit_perform_as(args.l)

        elif args.a == 'commit_label_links':
            doClient.commit_label_links(args.l)

        elif args.a == 'commit_release_links':
            doClient.commit_release_links(args.l)
        elif args.a == 'commit_release_credits':
            doClient.commit_release_artist_relationship(args.l)
        elif args.a == 'commit_release_format':
            doClient.commit_release_format(args.l)
        elif args.a == 'commit_release_barcode':
            doClient.commit_release_barcode(args.l)

        elif args.a == 'commit_recording_credits':
            doClient.commit_recording_credits(args.l)

        elif args.a == 'run_artist_404':
            doClient.run_artist_404(args.l)
        elif args.a == 'run_label_404':
            doClient.run_label_404(args.l)
        elif args.a == 'run_artist_types':
            doClient.run_artist_types(args.l)
        elif args.a == 'run_artist_country':
            doClient.run_artist_country(args.l)
        elif args.a == 'run_artist_country2':
            doClient.run_artist_country2(args.l)
    else:
        parser.print_help()
