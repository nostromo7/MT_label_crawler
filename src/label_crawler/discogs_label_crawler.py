import sys
import os
import pickle
import pandas as pd
import discogs_client
import json
import numpy as np
from tqdm import tqdm
import re
import requests
from enum import Enum

from src import constants
from src import discogs_credentials

# get constants
INPUT_LABEL_MAP = constants.LABEL_MAP_TRIVIAL
OUTPUT_LABEL_MAP = constants.LABEL_MAP_DISCOGS
OUTPUT_LABEL_MAP_EXT = constants.LABEL_MAP_DISCOGS_EXT

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
CLASS_TRIVIAL = constants.CLASS_TRIVIAL
CLASS_DISCOGS = constants.CLASS_DISCOGS
DISCOGS_WIKI_URL = constants.DISCOGS_WIKI_URL
DISCOGS_KEYWORD_UNIV_SUM = constants.DISCOGS_KEYWORD_UNIV_SUM
DISCOGS_KEYWORD_SONY_SUM = constants.DISCOGS_KEYWORD_SONY_SUM
DISCOGS_KEYWORD_WARN_SUM = constants.DISCOGS_KEYWORD_WARN_SUM
DISCOGS_KEYWORD_INDI_SUM = constants.DISCOGS_KEYWORD_INDI_SUM

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

DISCOGS_FAIL_FLAG = constants.DISCOGS_FAIL_FLAG
DISCOGS_TRY_FLAG = constants.DISCOGS_TRY_FLAG
DISCOGS_NO_ID_FLAG = constants.DISCOGS_NO_ID_FLAG
DISCOGS_NO_PAR_FLAG = constants.DISCOGS_NO_PAR_FLAG
DISCOGS_MAX_DEPTH = constants.DISCOGS_MAX_DEPTH

# global variables
FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]
DISCOGS_FLAGS = [DISCOGS_FAIL_FLAG, DISCOGS_TRY_FLAG, DISCOGS_NO_ID_FLAG, DISCOGS_NO_PAR_FLAG, DISCOGS_MAX_DEPTH]

ARCHIVE_LOOKUP_PATH = constants.ARCHIVE_DISCOGS_LOOKUP_PATH
ARCHIVE_DISCOGS_ID_MAP_PATH = constants.ARCHIVE_DISCOGS_ID_MAP_PATH

# dict: <label_id, DiscogsEntry>
ARCHIVE_LOOKUP = {}
# dict: <label_name, label_id>
ARCHIVE_ID_MAP = {}

UNIV_DISCOGS_ID = 38404
SONY_DISCOGS_ID = 353657
WARN_DISCOGS_ID = 2345

MAJORS_DICT = {
    'Universal': UNIV_DISCOGS_ID,
    'Sony': SONY_DISCOGS_ID,
    'Warner': WARN_DISCOGS_ID
}

LABEL_MAP = pd.DataFrame()
DISCOGS_CLIENT = discogs_client.Client(discogs_credentials.user_agent,
                                       user_token=discogs_credentials.discogs_token)

DEBUG = constants.DEBUG
STOP_AT = None
SAVE_AFTER = 1000
MAX_DEPTH = 6


class Classification(Enum):
    DISCOGS_FAIL_FLAG = DISCOGS_FAIL_FLAG
    DISCOGS_MAX_DEPTH = DISCOGS_MAX_DEPTH
    DISCOGS_TRY_FLAG = DISCOGS_TRY_FLAG
    DISCOGS_NO_ID_FLAG = DISCOGS_NO_ID_FLAG
    DISCOGS_NO_PAR_FLAG = DISCOGS_NO_PAR_FLAG

    FINAL_UNIV = FINAL_UNIV
    FINAL_SONY = FINAL_SONY
    FINAL_WARN = FINAL_WARN
    FINAL_INDI = FINAL_INDI
    FINAL_UNKN = FINAL_UNKN


# this class captures all relevant information of a discogs page
class DiscogsEntry:
    def __init__(self, shortcut: Classification = DISCOGS_FAIL_FLAG):
        self.shortcut = shortcut
    # shortcut to end of discogs chain
    shortcut = DISCOGS_FAIL_FLAG
    # id of parent label, leading to next entry in archive
    parent_label = None
    # Name of the label for understandability
    label_name = None
    # description of discogs page, used for keyword search
    description = None
    # keyword collection
    keywords = {}
    # if wikipedia page is listed
    wiki_page = None


def run_crawler(input_map=INPUT_LABEL_MAP, index_from=-1):
    global LABEL_MAP

    if DEBUG: print('load:', input_map)
    if input_map == INPUT_LABEL_MAP:
        LABEL_MAP = pd.read_csv(input_map)
        LABEL_MAP[CLASS_DISCOGS] = LABEL_MAP[CLASS_TRIVIAL]
        LABEL_MAP[DISCOGS_WIKI_URL] = np.nan
        LABEL_MAP[[DISCOGS_KEYWORD_UNIV_SUM, DISCOGS_KEYWORD_SONY_SUM, DISCOGS_KEYWORD_WARN_SUM, DISCOGS_KEYWORD_INDI_SUM]] = [0, 0, 0, 0]
    else:
        LABEL_MAP = pd.read_csv(input_map, dtype={DISCOGS_WIKI_URL: str})


    for index, entry in tqdm(LABEL_MAP.iterrows(), total=LABEL_MAP.shape[0]):
    # for index, entry in LABEL_MAP.iterrows():
        if STOP_AT is not None and entry[RECORD_LABEL_LOW] != STOP_AT:
            continue

        if index > index_from and entry[CLASS_DISCOGS] not in FINALS and entry[CLASS_DISCOGS] not in DISCOGS_FLAGS:
            if DEBUG: print('------------------')
            if DEBUG: print('start lookup for: ', entry[RECORD_LABEL_LOW])
            discogs_entry = get_major_label_classification(entry[RECORD_LABEL_LOW])

            keyword_aggregate = aggregate_keywords(discogs_entry)
            LABEL_MAP.loc[index, [CLASS_DISCOGS, DISCOGS_WIKI_URL]] = [discogs_entry.shortcut, discogs_entry.wiki_page]
            LABEL_MAP.loc[index, DISCOGS_KEYWORD_UNIV_SUM] = keyword_aggregate['universal'] if 'universal' in keyword_aggregate.keys() else 0
            LABEL_MAP.loc[index, DISCOGS_KEYWORD_SONY_SUM] = keyword_aggregate['sony'] if 'sony' in keyword_aggregate.keys() else 0
            LABEL_MAP.loc[index, DISCOGS_KEYWORD_WARN_SUM] = keyword_aggregate['warner'] if 'warner' in keyword_aggregate.keys() else 0
            LABEL_MAP.loc[index, DISCOGS_KEYWORD_INDI_SUM] = keyword_aggregate['independent'] if 'independent' in keyword_aggregate.keys() else 0

            if DEBUG: print(entry[RECORD_LABEL_LOW], ' --> ', discogs_entry.shortcut, keyword_aggregate)

        if index > index_from and index % SAVE_AFTER == 0:
            if DEBUG: print('saving at', index)
            save_label_map()
            save_archives()

    save_label_map()
    save_archives()

def get_major_label_classification(label_name) -> DiscogsEntry:
    global ARCHIVE_LOOKUP

    label_id = get_discogs_label_id(label_name)

    # check if label was found
    if label_id in DISCOGS_FLAGS:
        return DiscogsEntry(label_id)

    elif label_id is not None:
        if label_id not in ARCHIVE_LOOKUP.keys():
            ARCHIVE_LOOKUP[label_id] = extract_discogs_page(label_id)

        return ARCHIVE_LOOKUP[label_id]
    else:
        return DiscogsEntry(DISCOGS_NO_ID_FLAG)


def get_discogs_label_id(label_name):
    global ARCHIVE_ID_MAP

    if label_name in ARCHIVE_ID_MAP.keys():
        return ARCHIVE_ID_MAP[label_name]

    else:
        try:
            res = DISCOGS_CLIENT.search(label_name, type='label', page=1, per_page=5)

            if len(res.page(0)) > 0:
                # get first entry of first page of result
                label_id = res.page(0)[0].id
                ARCHIVE_ID_MAP[label_name] = label_id
                return label_id
            else:
                if DEBUG: print('No id found for', label_name)
                ARCHIVE_ID_MAP[label_name] = DISCOGS_NO_ID_FLAG
                return None
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as e:
            # happened first on 'South By Sea Music'
            if DEBUG: print('Connection Error: ', e)
            # TODO: change this and set a different flag for possible retry later; check: is a retry even successful or are the same labels retried every time?
            ARCHIVE_ID_MAP[label_name] = DISCOGS_NO_ID_FLAG
            return None


def extract_discogs_page(label_id, depth=0) -> DiscogsEntry:
    global ARCHIVE_LOOKUP

    if depth > MAX_DEPTH:
        if DEBUG: print('Max depth reached for', label_id)
        return DiscogsEntry(DISCOGS_MAX_DEPTH)

    # check if label has already been loaded
    label_full = DISCOGS_CLIENT.label(label_id)

    # create new entry object
    discogs_entry = DiscogsEntry(DISCOGS_TRY_FLAG)
    # get name & description
    try:
        discogs_entry.label_name = label_full.name
        discogs_entry.description = label_full.profile

        if label_full.urls is not None:
            for url in label_full.urls:
                if 'wikipedia' in url:
                    discogs_entry.wiki_page = str(url)
                    break

        # check if parent label is listed
        try:
            parent_id = label_full.parent_label.id
            discogs_entry.parent_label = parent_id

            # check for matches for major IDs
            if parent_id == UNIV_DISCOGS_ID:
                discogs_entry.shortcut = FINAL_UNIV
                return discogs_entry
            if parent_id == SONY_DISCOGS_ID:
                discogs_entry.shortcut = FINAL_SONY
                return discogs_entry
            if parent_id == WARN_DISCOGS_ID:
                discogs_entry.shortcut = FINAL_WARN
                return discogs_entry

            # if no match occurred and a parent exists, repeat lookup for parent
            if parent_id not in ARCHIVE_LOOKUP.keys():
                if DEBUG: print('Start recursive lookup for:', parent_id)
                ARCHIVE_LOOKUP[parent_id] = extract_discogs_page(parent_id, depth + 1)
            # set shortcut of parent also for child
            discogs_entry.shortcut = ARCHIVE_LOOKUP[parent_id].shortcut
            # if no wikipage has been found yet, take the one from parent
            if ARCHIVE_LOOKUP[parent_id].wiki_page is not None:
                discogs_entry.wiki_page = ARCHIVE_LOOKUP[parent_id].wiki_page

        # if no parent label is listed, check if profile text contains 'independent' keyword
        except AttributeError as e:
            discogs_entry.shortcut = DISCOGS_NO_PAR_FLAG
            if DEBUG: print('Attribute Error´:', e)

    except requests.exceptions.ConnectionError as e:
        discogs_entry.shortcut = DISCOGS_FAIL_FLAG
        if DEBUG: print('Connection Error:', e)

    finally:
        # TODO: Strict independent classification is enabled
        # check for independent keyword when parent lookups were unsuccessful
        # if discogs_entry.shortcut in DISCOGS_FLAGS and indi_keyword_in_text(label_full.profile):
        #     discogs_entry.shortcut = FINAL_INDI

        discogs_entry.keywords = count_keywords(discogs_entry.description)

        return discogs_entry


def count_keywords(description):
    collection = {
        'universal': 0,
        'sony': 0,
        'warner': 0,
        'independent': 0
    }

    for keyword in collection.keys():
        if description is not None:
            collection[keyword] = description.lower().count(keyword)

    return collection


def aggregate_keywords(entry: DiscogsEntry, depth=0):
    res_collection = entry.keywords

    if depth > MAX_DEPTH:
        return res_collection

    if entry.parent_label is not None:
        if entry.parent_label in ARCHIVE_LOOKUP.keys():
            res_collection = merge_keywords_collections(res_collection, aggregate_keywords(ARCHIVE_LOOKUP[entry.parent_label], depth + 1), depth + 1)

    return res_collection


def merge_keywords_collections(col1, col2, depth):
    res_collection = {
        'universal': 0,
        'sony': 0,
        'warner': 0,
        'independent': 0
    }

    for key in res_collection.keys():
        if key in col1.keys() and key in col2.keys():
            res_collection[key] = (col1[key] + col2[key]) / (2 ** depth)

    return res_collection


def indi_keyword_in_text(description):
    if description is not None and re.search('independent', description, re.IGNORECASE) is not None:
        return True
    else:
        return False


def load_archives():
    global ARCHIVE_LOOKUP, ARCHIVE_ID_MAP
    try:
        with open(ARCHIVE_LOOKUP_PATH, 'rb+') as read_file:
            ARCHIVE_LOOKUP = pickle.load(read_file)
            if DEBUG: print('using existing archive', ARCHIVE_LOOKUP_PATH)
    except FileNotFoundError as e:
        if DEBUG: print('No archive existed at', ARCHIVE_LOOKUP_PATH, ', creating an empty one.')
        ARCHIVE_LOOKUP = {}
    try:
        with open(ARCHIVE_DISCOGS_ID_MAP_PATH, 'rb+') as read_file:
            ARCHIVE_ID_MAP = pickle.load(read_file)
            if DEBUG: print('using existing archive', ARCHIVE_DISCOGS_ID_MAP_PATH)
    except FileNotFoundError as e:
        if DEBUG: print('No id_archive existed at', ARCHIVE_DISCOGS_ID_MAP_PATH, ', creating an empty one.')
        ARCHIVE_ID_MAP = {}


def save_archives():
    if DEBUG: print('saving archives')
    with open(ARCHIVE_LOOKUP_PATH, 'wb+') as write_file:
        pickle.dump(ARCHIVE_LOOKUP, write_file)
    with open(ARCHIVE_DISCOGS_ID_MAP_PATH, 'wb+') as write_file:
        pickle.dump(ARCHIVE_ID_MAP, write_file)


def save_label_map():
    if DEBUG: print('Saving df')
    LABEL_MAP.to_csv(OUTPUT_LABEL_MAP_EXT, index=False)
    LABEL_MAP.drop([DISCOGS_WIKI_URL, DISCOGS_KEYWORD_UNIV_SUM, DISCOGS_KEYWORD_SONY_SUM, DISCOGS_KEYWORD_WARN_SUM, DISCOGS_KEYWORD_INDI_SUM], axis=1).to_csv(OUTPUT_LABEL_MAP, index=False)


def main(debug=None):
    global INPUT_LABEL_MAP, OUTPUT_LABEL_MAP, ARCHIVE_LOOKUP_PATH, ARCHIVE_DISCOGS_ID_MAP_PATH, DEBUG

    print()
    print('##########################################################')
    print('###          CRAWLER STEP 2: DISCOGS CRAWLER           ###')
    print('##########################################################')

    if debug is not None:
        DEBUG = debug

    load_archives()
    if os.path.exists(OUTPUT_LABEL_MAP_EXT):
        run_crawler(OUTPUT_LABEL_MAP_EXT)
    else:
        run_crawler(INPUT_LABEL_MAP)

    return 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        save_label_map()
        save_archives()
        sys.exit(-1)
