import getopt
import sys

from src.label_crawler import label_mapping_trivial
from src.label_crawler import discogs_label_crawler
from src.label_crawler import wikipedia_crawler
from src.label_crawler import label_mapping_interim
from src.label_crawler import label_copyright_classifier
from src.label_crawler import label_mapping_final
from src.label_crawler import label_classification_analysis
from src.label_crawler import label_crawler_postprocessing

# defines if the analysis of the gathered record label data is run or not
RUN_ANALYSIS = False

# show or hide detailed output of crawler steps
DEBUG = False

# defines if a list of album or track uris was used as starting point for the preprocessing. This is necessary for the
# crawler postprocessing, mapping the final classification back to maps of album and (optional) track URI
TRACK_START = False


def main(debug=None, track_start=None, run_analysis=None):
    global DEBUG, RUN_ANALYSIS, TRACK_START

    if debug is not None:
        DEBUG = debug
    if track_start is not None:
        TRACK_START = track_start
    if run_analysis is not None:
        RUN_ANALYSIS = run_analysis

    label_mapping_trivial.main(debug=DEBUG)
    discogs_label_crawler.main(debug=DEBUG, max_depth=6)
    wikipedia_crawler.main(debug=DEBUG, max_depth=6)
    label_mapping_interim.main(debug=DEBUG, wiki_keyword_agg_under=2, wiki_keyword_agg_over=0.25)
    label_copyright_classifier.main(debug=DEBUG)
    label_mapping_final.main(debug=DEBUG, discogs_keyword_agg_over=0.2)
    label_crawler_postprocessing.main(track_start=TRACK_START)

    if RUN_ANALYSIS:
        label_classification_analysis.main()


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    options = 'da'
    long_options = ['debug', 'run_analysis']

    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)

        for curr_arg, curr_value in arguments:
            if curr_arg in ('-d', '--debug'):
                DEBUG = True
            if curr_arg in ('-a', '--run_analysis'):
                RUN_ANALYSIS = True

        main()

    except getopt.error as err:
        print(str(err))
        sys.exit(-1)
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
