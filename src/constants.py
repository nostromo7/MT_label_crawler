import os
import getopt
import sys
from src import config

dirname = os.path.dirname(__file__)


# ##############################
# ##### General constants ######
# ##############################

# The dataset tag defines the suffix for all output files, this comes in handy when the crawler is run
# on multiple datasets in parallel
DATASET_TAG = config.DATASET_TAG

DEBUG = False

# path to folder of MPD dataset
PATH_TO_SLICES_ORIGINAL = os.path.join(dirname, '../data/slices_original/')
PATH_TO_SLICES_ENRICHED = os.path.join(dirname, '../data/slices_enriched/')

# path to lfm-2b file
PATH_TO_LFM_ORIGINAL = os.path.join(dirname, '../data/lfm-2b_original/spotify-uris.tsv')
PATH_TO_LFM_ENRICHED = os.path.join(dirname, '../data/lfm-2b_original/spotify-uris_enriched.tsv')


# ######################################
# ##### Spotify crawler constants ######
# ######################################

# spotify crawler constants
LAST_SAVE_INDEX = -1
SAVING_STEP = 5000
ALBUM_REQUEST_BULK_SIZE = 20

# Flags for failed lookups
BULK_FAILED_FLAG = 'Error: Bulk lookup failed (404)'
FAILED_LOOKUP_FLAG = 'Error: Lookup failed (404)'

# columns for spotify crawler
TRACK_URI = 'track_uri'
ALBUM_URI = 'album_uri'
OCC = 'occurrences'
RECORD_LABEL_LOW = 'record_label_low'
COPYRIGHT_P = 'copyright_p'
COPYRIGHT_C = 'copyright_c'

# ######## Generic file paths for preprocessing: ########

# input file for as list of track_uris with optional occurrences:
INPUT_TRACK_URIS = os.path.join(dirname, '../data/generated/sorted_track_uris' + DATASET_TAG + '.csv')

# output file for list of track_uris with album_uris:
OUTPUT_TRACK_URIS = os.path.join(dirname, '../data/generated/sorted_track_uris_with_album_uris' + DATASET_TAG + '.csv')

# output files for spotify crawler preprocessing output
SORTED_ALBUM_URIS = os.path.join(dirname, '../data/generated/sorted_album_uris' + DATASET_TAG + '.csv')

# output files for spotify crawler
ALBUM_URIS_WITH_LABEL_LOW = os.path.join(dirname, '../data/generated/sorted_album_uris_with_low_label' + DATASET_TAG + '.csv')

# output files for spotify crawler analysis
SORTED_LOW_LABEL_OUTPUT = os.path.join(dirname,  '../data/generated/sorted_low_labels' + DATASET_TAG + '.csv')

# ######## File paths for LFM-2b: ########

LFM_WITH_ALBUM_URIS = os.path.join(dirname, '../data/generated/sorted_album_uris' + DATASET_TAG + '.csv')
LFM_ALBUM_URIS_WITH_LABEL_LOW = os.path.join(dirname, '../data/generated/sorted_album_uris_with_low_label' + DATASET_TAG + '.csv')

###########################################
# ######## output files of spotify postprocessing


# output file spotify postprocessing
LABEL_MAP = os.path.join(dirname, '../data/generated/label_map' + DATASET_TAG + '.csv')
COPYRIGHT_MAP = os.path.join(dirname, '../data/generated/copyright_map' + DATASET_TAG + '.csv')


# ################################################
# ##### Multi-stage label crawler constants ######
# ################################################

# ##### General constants for label crawling ######

# Defined target classes
FINAL_UNIV = 'Universal Music Group'
FINAL_SONY = 'Sony Music Entertainment'
FINAL_WARN = 'Warner Records'
FINAL_INDI = 'Independent'
FINAL_UNKN = 'Unknown'


# ##### Trivial mapping ######

# Defined list of alias terms for each target
# Note: These alias are derived from studying the MPD
UNIV_ALIAS = r'\b(?:Universal|Capitol)\b'
SONY_ALIAS = r'\b(?:Sony|RCA|Columbia|Epic/|/Epic)\b'
WARN_ALIAS = r'\b(?:Warner|WM|Atlantic Records|Rhino)\b'
INDI_ALIAS = r'\b(?:Independent)\b'
UNKN_ALIAS = r'\b(?:Unknown|N/A|None|N/A (Independent)|Unknown Label|Unsigned|Various Artists|Various Artist|Vintage Music)|Error: Lookup failed (404)\b'

# New column for trivial mapping classification
CLASS_TRIVIAL = 'class_trivial'

# Output of trivial mapping
LABEL_MAP_TRIVIAL = os.path.join(dirname, '../data/generated/label_map_trivial' + DATASET_TAG + '.csv')


# ##### Discogs label crawler ######

# flags for discogs crawler
DISCOGS_TRY_FLAG = 'Discogs: Unsuccessful lookup'
DISCOGS_NO_ID_FLAG = 'Discogs: Label not found'
DISCOGS_NO_PAR_FLAG = 'Discogs: No parent found'
DISCOGS_MAX_DEPTH = 'Discogs: Exceeded max depth'
DISCOGS_FAIL_FLAG = 'Discogs: Failed lookup'

# New column for discogs crawler classification
CLASS_DISCOGS = 'class_discogs'
DISCOGS_WIKI_URL = 'discogs_wiki_url'
DISCOGS_KEYWORD_UNIV_SUM = 'discogs_keywords_univ_sum'
DISCOGS_KEYWORD_SONY_SUM = 'discogs_keywords_sony_sum'
DISCOGS_KEYWORD_WARN_SUM = 'discogs_keywords_warn_sum'
DISCOGS_KEYWORD_INDI_SUM = 'discogs_keywords_indi_sum'

# Output of discogs crawler
LABEL_MAP_DISCOGS = os.path.join(dirname, '../data/generated/label_map_discogs' + DATASET_TAG + '.csv')
LABEL_MAP_DISCOGS_EXT = os.path.join(dirname, '../data/generated/label_map_discogs_ext' + DATASET_TAG + '.csv')
ARCHIVE_DISCOGS_LOOKUP_PATH = os.path.join(dirname, '../data/generated/archive_discogs_lookup' + DATASET_TAG + '.pkl')
ARCHIVE_DISCOGS_ID_MAP_PATH = os.path.join(dirname, '../data/generated/archive_discogs_id_map' + DATASET_TAG + '.pkl')


# ##### Wikipedia label crawler ######

# flags for wikipedia crawler
WIKI_TRY_FLAG = 'Wikipedia: Unsuccessful lookup'
WIKI_NO_URL_FLAG = 'Wikipedia: No Wiki url found'
WIKI_DE_FLAG = 'Wikipedia: Dead end'  # (no parents, distributors or infobox)
WIKI_MAX_DEPTH = 'Wikipedia: Exceeded max depth'
WIKI_FAIL_FLAG = 'Wikipedia: Failed lookup'
WIKI_ERROR_FLAG_CONNECTION = 'Wikipedia: Connection Error'
WIKI_ERROR_FLAG_PAGE = 'Wikipedia: Page Error'
WIKI_ERROR_FLAG_DISAMBIGUATION = 'Wikipedia: Disambiguation Error'
WIKI_ERROR_FLAG_REDIRECT = 'Wikipedia: Redirect Error'
WIKI_ERROR_FLAG_WIKI_EXCEPTION = 'Wikipedia: Wikipedia Exception (Search too busy)'

# New column for wikipedia crawler classification
CLASS_WIKIPEDIA = 'class_wikipedia'
WIKI_URL = 'wiki_url'
WIKI_HAS_INDI_LINK = 'wiki_has_indi_link'
WIKI_KEYWORD_UNIV_SUM = 'wiki_keywords_univ_sum'
WIKI_KEYWORD_SONY_SUM = 'wiki_keywords_sony_sum'
WIKI_KEYWORD_WARN_SUM = 'wiki_keywords_warn_sum'
WIKI_KEYWORD_INDI_SUM = 'wiki_keywords_indi_sum'

# Wikipedia output columns
LABEL_MAP_WIKIPEDIA = os.path.join(dirname, '../data/generated/label_map_wikipedia' + DATASET_TAG + '.csv')
LABEL_MAP_WIKIPEDIA_EXT = os.path.join(dirname, '../data/generated/label_map_wikipedia_ext' + DATASET_TAG + '.csv')
ARCHIVE_WIKIPEDIA_LOOKUP_PATH = os.path.join(dirname, '../data/generated/archive_wiki_lookup' + DATASET_TAG + '.pkl')
ARCHIVE_WIKIPEDIA_URL_MAP_PATH = os.path.join(dirname, '../data/generated/archive_wiki_url_map' + DATASET_TAG + '.pkl')


# ##### Interim mapping ######

# New column for trivial mapping classification
CLASS_INTERIM = 'class_interim'

# Output of trivial mapping
LABEL_MAP_INTERIM = os.path.join(dirname, '../data/generated/label_map_interim' + DATASET_TAG + '.csv')


# ##### Copyright classification ######

# New column for trivial mapping classification
CLASS_COPYRIGHT = 'class_copyright'

# Output of trivial mapping
LABEL_MAP_COPYRIGHT = os.path.join(dirname, '../data/generated/label_map_copyright' + DATASET_TAG + '.csv')
LABEL_MAP_COPYRIGHT_INTERIM = os.path.join(dirname, '../data/generated/label_map_copyright_interim' + DATASET_TAG + '.csv')

# ##### Final classification ######

# New column for trivial mapping classification
RECORD_LABEL_MAJOR = 'record_label_major'

# Output of trivial mapping
LABEL_MAP_FINAL = os.path.join(dirname, '../data/generated/label_map_final' + DATASET_TAG + '.csv')
LABEL_MAP_FINAL_STATS = os.path.join(dirname, '../data/generated/label_map_final_stats' + DATASET_TAG + '.csv')

# ##### Postprocessing #####

ALBUM_URIS_ENRICHED = os.path.join(dirname, '../data/generated/sorted_album_uris_enriched' + DATASET_TAG + '.csv')
TRACK_URIS_ENRICHED = os.path.join(dirname, '../data/generated/sorted_track_uris_enriched' + DATASET_TAG + '.csv')


def recreate_tag_dependant_constants():
    sys.exit(11)
    global PATH_TO_SLICES_ORIGINAL, PATH_TO_SLICES_ENRICHED, PATH_TO_LFM_ORIGINAL, PATH_TO_LFM_ENRICHED, INPUT_TRACK_URIS, OUTPUT_TRACK_URIS, SORTED_ALBUM_URIS, ALBUM_URIS_WITH_LABEL_LOW, SORTED_LOW_LABEL_OUTPUT, LFM_WITH_ALBUM_URIS, LFM_ALBUM_URIS_WITH_LABEL_LOW, LABEL_MAP, COPYRIGHT_MAP, LABEL_MAP_TRIVIAL, LABEL_MAP_DISCOGS, LABEL_MAP_DISCOGS_EXT, ARCHIVE_DISCOGS_LOOKUP_PATH, ARCHIVE_DISCOGS_ID_MAP_PATH, LABEL_MAP_WIKIPEDIA, LABEL_MAP_WIKIPEDIA_EXT, ARCHIVE_WIKIPEDIA_LOOKUP_PATH, ARCHIVE_WIKIPEDIA_URL_MAP_PATH, LABEL_MAP_INTERIM, LABEL_MAP_COPYRIGHT, LABEL_MAP_COPYRIGHT_INTERIM, LABEL_MAP_FINAL, LABEL_MAP_FINAL_STATS, ALBUM_URIS_WITH_FINAL_CLASSIFICATION


    PATH_TO_SLICES_ORIGINAL = os.path.join(dirname, '../data/slices_original/')
    PATH_TO_SLICES_ENRICHED = os.path.join(dirname, '../data/slices_enriched/')

    # path to lfm-2b file
    PATH_TO_LFM_ORIGINAL = os.path.join(dirname, '../data/lfm-2b_original/spotify-uris.tsv')
    PATH_TO_LFM_ENRICHED = os.path.join(dirname, '../data/lfm-2b_original/spotify-uris_enriched.tsv')

    # input file for as list of track_uris with optional occurrences:
    INPUT_TRACK_URIS = os.path.join(dirname, '../data/generated/sorted_track_uris' + DATASET_TAG + '.csv')

    # output file for list of track_uris with album_uris:
    OUTPUT_TRACK_URIS = os.path.join(dirname, '../data/generated/sorted_track_uris_with_album_uris' + DATASET_TAG + '.csv')

    # output files for spotify crawler preprocessing output
    SORTED_ALBUM_URIS = os.path.join(dirname, '../data/generated/sorted_album_uris' + DATASET_TAG + '.csv')

    # output files for spotify crawler
    ALBUM_URIS_WITH_LABEL_LOW = os.path.join(dirname, '../data/generated/sorted_albums_with_low_label' + DATASET_TAG + '.csv')

    # output files for spotify crawler analysis
    SORTED_LOW_LABEL_OUTPUT = os.path.join(dirname,  '../data/generated/sorted_low_labels' + DATASET_TAG + '.csv')

    # ######## File paths for LFM-2b: ########

    LFM_WITH_ALBUM_URIS = os.path.join(dirname, '../data/generated/album_uris_lfm' + DATASET_TAG + '.csv')
    LFM_ALBUM_URIS_WITH_LABEL_LOW = os.path.join(dirname, '../data/generated/albums_uri_with_low_label_lfm' + DATASET_TAG + '.csv')

    ###########################################
    # ######## output files of spotify postprocessing

    # output file spotify postprocessing
    LABEL_MAP = os.path.join(dirname, '../data/generated/label_map' + DATASET_TAG + '.csv')
    COPYRIGHT_MAP = os.path.join(dirname, '../data/generated/copyright_map' + DATASET_TAG + '.csv')

    # ################################################
    # ##### Multi-stage label crawler constants ######
    # ################################################

    # Output of trivial mapping
    LABEL_MAP_TRIVIAL = os.path.join(dirname, '../data/generated/label_map_trivial' + DATASET_TAG + '.csv')

    # Output of discogs crawler
    LABEL_MAP_DISCOGS = os.path.join(dirname, '../data/generated/label_map_discogs' + DATASET_TAG + '.csv')
    LABEL_MAP_DISCOGS_EXT = os.path.join(dirname, '../data/generated/label_map_discogs_ext' + DATASET_TAG + '.csv')
    ARCHIVE_DISCOGS_LOOKUP_PATH = os.path.join(dirname, '../data/generated/archive_discogs_lookup' + DATASET_TAG + '.pkl')
    ARCHIVE_DISCOGS_ID_MAP_PATH = os.path.join(dirname, '../data/generated/archive_discogs_id_map' + DATASET_TAG + '.pkl')

    # Wikipedia output columns
    LABEL_MAP_WIKIPEDIA = os.path.join(dirname, '../data/generated/label_map_wikipedia' + DATASET_TAG + '.csv')
    LABEL_MAP_WIKIPEDIA_EXT = os.path.join(dirname, '../data/generated/label_map_wikipedia_ext' + DATASET_TAG + '.csv')
    ARCHIVE_WIKIPEDIA_LOOKUP_PATH = os.path.join(dirname, '../data/generated/archive_wiki_lookup' + DATASET_TAG + '.pkl')
    ARCHIVE_WIKIPEDIA_URL_MAP_PATH = os.path.join(dirname, '../data/generated/archive_wiki_url_map' + DATASET_TAG + '.pkl')

    # Output of trivial mapping
    LABEL_MAP_INTERIM = os.path.join(dirname, '../data/generated/label_map_interim' + DATASET_TAG + '.csv')

    # Output of trivial mapping
    LABEL_MAP_COPYRIGHT = os.path.join(dirname, '../data/generated/label_map_copyright' + DATASET_TAG + '.csv')
    LABEL_MAP_COPYRIGHT_INTERIM = os.path.join(dirname, '../data/generated/label_map_copyright_interim' + DATASET_TAG + '.csv')

    # Output of trivial mapping
    LABEL_MAP_FINAL = os.path.join(dirname, '../data/generated/label_map_final' + DATASET_TAG + '.csv')
    LABEL_MAP_FINAL_STATS = os.path.join(dirname, '../data/generated/label_map_final_stats' + DATASET_TAG + '.csv')

    # Output for map of album_uris with both low and major record label
    ALBUM_URIS_WITH_FINAL_CLASSIFICATION = os.path.join(dirname, '../data/generated/sorted_albums_with_final_classification' + DATASET_TAG + '.csv')


def set_constants(dataset_tag=None):
    sys.exit(11)
    global DATASET_TAG

    if dataset_tag is not None:
        DATASET_TAG = '_' + dataset_tag

    print('CONSTANTS, tag set to: ', DATASET_TAG)


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    options = 'd'
    long_options = ['dataset_tag=']

    try:
        arguments, values = getopt.getopt(argument_list, options, long_options)
        dataset_tag = None

        for curr_arg, curr_value in arguments:
            if curr_arg in ('-d', '--dataset_tag'):
                dataset_tag = curr_value

        set_constants(dataset_tag)

    except getopt.error as err:
        print(str(err))
        sys.exit(-1)
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)