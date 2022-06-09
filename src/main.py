import getopt
import sys

from src.preprocessing_spotify import run_preprocessing
from src.label_crawler import run_label_crawler


def main(path_to_label_map=None, track_start=False, analysis=False, debug=False):
    if track_start:
        print('Start from list of track_uris')
    else:
        print('Start from list of album_uris')
    run_preprocessing.main(path_to_label_map=path_to_label_map, start_from_track_uris=track_start,
                           run_analysis=analysis, skip_preprocessing=skip_preprocessing, debug=debug)

    run_label_crawler.main(run_analysis=analysis, track_start=track_start, debug=debug)


if __name__ == "__main__":

    argument_list = sys.argv[1:]
    options = 'darts'
    long_options = ['debug', 'analysis', 'reuse_label_map=', 'track_start', 'skip_preprocessing']

    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)
        track_start = False
        path_to_label_map = None
        run_analysis = False
        debug = False
        skip_preprocessing = False

        for curr_arg, curr_value in arguments:
            if curr_arg in ('-d', '--debug'):
                debug = True
            if curr_arg in ('-a', '--analysis'):
                run_analysis = True
            if curr_arg in ('-s', '--skip_preprocessing'):
                skip_preprocessing = True
            if curr_arg in ('-r', '--reuse_label_map'):
                path_to_label_map = curr_value
            if curr_arg in ('-t', '--track_start'):
                track_start = True

        main(path_to_label_map, track_start, run_analysis, debug)

    except getopt.error as err:
        print(str(err))
        sys.exit(-1)
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
