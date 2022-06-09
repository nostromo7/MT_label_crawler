import getopt
import sys

from src.preprocessing_spotify import spotify_album_crawler
from src.preprocessing_spotify import spotify_record_label_crawler
from src.preprocessing_spotify import spotify_crawler_postprocessing
from src.preprocessing_spotify import spotify_crawler_analysis

# if a label map from a previous run already exists set path here or as input to this script
PATH_TO_LABEL_MAP = None

# set to true if a list of spotify track_uris are used instead of album_uris
START_FROM_TRACK_URIS = False

# defines if the analysis of the gathered spotify data is run or not
RUN_ANALYSIS = False

# Skips preprocessing step, this is useful when a crawling run is being continued
SKIP_PREPROCESSING = False

# show or hide detailed output of crawler steps
DEBUG = False


def main(path_to_label_map=None, start_from_track_uris=None, run_analysis=None, skip_preprocessing=None, debug=None):
    global START_FROM_TRACK_URIS, PATH_TO_LABEL_MAP, RUN_ANALYSIS, DEBUG

    if start_from_track_uris is not None:
        START_FROM_TRACK_URIS = start_from_track_uris
    if path_to_label_map is not None:
        PATH_TO_LABEL_MAP = path_to_label_map
    if run_analysis is not None:
        RUN_ANALYSIS = run_analysis
    if debug is not None:
        DEBUG = debug

    if START_FROM_TRACK_URIS:
        print('Run spotify crawler for track_uri to album_uri ')
        spotify_album_crawler.main(debug=debug)

    print('Run spotify crawler for album_uri to label- and copyright info')
    spotify_record_label_crawler.main(debug=debug)

    if PATH_TO_LABEL_MAP is not None:
        print('Create low-level record label list and fill with classification from', PATH_TO_LABEL_MAP)
        spotify_crawler_postprocessing.main(existing_label_map=PATH_TO_LABEL_MAP)
    else:
        print('Create low-level record label list')
        spotify_crawler_postprocessing.main(debug=debug)

    if RUN_ANALYSIS:
        spotify_crawler_analysis.main()


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    options = 'dtla'
    long_options = ['debug', 'track_base', 'label_map=', 'run_analysis']

    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)

        for curr_arg, curr_value in arguments:
            if curr_arg in ('-d', '--debug'):
                DEBUG = True

            if curr_arg in ('-t', '--track_base'):
                START_FROM_TRACK_URIS = True

            if curr_arg in ('-l', '--label_map'):
                print('Using existing label map at:', curr_value)
                PATH_TO_LABEL_MAP = curr_value

            if curr_arg in ('-a', '--run_analysis'):
                print('Using existing label map at:', curr_value)
                RUN_ANALYSIS = True

        main()

    except getopt.error as err:
        print(str(err))
        sys.exit(-1)
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
