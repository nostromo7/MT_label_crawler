import sys
import os
import json
import pickle
import string
import pandas as pd
import wikipedia
from bs4 import BeautifulSoup
from datetime import date, datetime
import numpy as np
import re
import requests
import math
import urllib
from enum import Enum
from tqdm import tqdm

from src import constants

####### get constants ########

INPUT_LABEL_MAP = constants.LABEL_MAP_DISCOGS_EXT
OUTPUT_LABEL_MAP = constants.LABEL_MAP_WIKIPEDIA
OUTPUT_LABEL_MAP_EXT = constants.LABEL_MAP_WIKIPEDIA_EXT

ARCHIVE_WIKIPEDIA_LOOKUP_PATH = constants.ARCHIVE_WIKIPEDIA_LOOKUP_PATH
ARCHIVE_WIKIPEDIA_URL_MAP_PATH = constants.ARCHIVE_WIKIPEDIA_URL_MAP_PATH

ARCHIVE_LOOKUP = {}
ARCHIVE_URL_MAP = {}

# columns combining discogs and wikipedia crawler
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
CLASS_TRIVIAL = constants.CLASS_TRIVIAL
CLASS_DISCOGS = constants.CLASS_DISCOGS
DISCOGS_WIKI_URL = constants.DISCOGS_WIKI_URL
CLASS_WIKIPEDIA = constants.CLASS_WIKIPEDIA

# new columns of wikipedia crawler
WIKI_URL = constants.WIKI_URL
WIKI_HAS_INDI_LINK = constants.WIKI_HAS_INDI_LINK
WIKI_KEYWORD_UNIV_SUM = constants.WIKI_KEYWORD_UNIV_SUM
WIKI_KEYWORD_SONY_SUM = constants.WIKI_KEYWORD_SONY_SUM
WIKI_KEYWORD_WARN_SUM = constants.WIKI_KEYWORD_WARN_SUM
WIKI_KEYWORD_INDI_SUM = constants.WIKI_KEYWORD_INDI_SUM

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

WIKI_TRY_FLAG = constants.WIKI_TRY_FLAG
WIKI_NO_URL_FLAG = constants.WIKI_NO_URL_FLAG
WIKI_DE_FLAG = constants.WIKI_DE_FLAG
WIKI_MAX_DEPTH = constants.WIKI_MAX_DEPTH
WIKI_FAIL_FLAG = constants.WIKI_FAIL_FLAG
WIKI_ERROR_FLAG_CONNECTION = constants.WIKI_ERROR_FLAG_CONNECTION
WIKI_ERROR_FLAG_PAGE = constants.WIKI_ERROR_FLAG_PAGE
WIKI_ERROR_FLAG_DISAMBIGUATION = constants.WIKI_ERROR_FLAG_DISAMBIGUATION
WIKI_ERROR_FLAG_REDIRECT = constants.WIKI_ERROR_FLAG_REDIRECT
WIKI_ERROR_FLAG_WIKI_EXCEPTION = constants.WIKI_ERROR_FLAG_WIKI_EXCEPTION

# global variables
FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]
DISCOGS_FLAGS = [DISCOGS_FAIL_FLAG, DISCOGS_TRY_FLAG, DISCOGS_NO_ID_FLAG, DISCOGS_NO_PAR_FLAG, DISCOGS_MAX_DEPTH]
WIKI_FLAGS = [WIKI_TRY_FLAG, WIKI_NO_URL_FLAG, WIKI_DE_FLAG, WIKI_MAX_DEPTH, WIKI_FAIL_FLAG,
              WIKI_ERROR_FLAG_CONNECTION, WIKI_ERROR_FLAG_PAGE, WIKI_ERROR_FLAG_DISAMBIGUATION, WIKI_ERROR_FLAG_REDIRECT]

CHECK_FOR_YEAR = datetime.strptime('2018', '%Y')
THIS_YEAR = str(date.today().year)

# get redirects: https://en.wikipedia.org/w/api.php?action=query&titles=Sony_Music&prop=redirects&format=jsonfm
UNIV_WIKI_URLS = ['/wiki/Universal_Music_Group', '/wiki/Universal_Music', '/wiki/UTV_Records', '/wiki/UMG']
SONY_WIKI_URLS = ['/wiki/Sony_Music', '/wiki/Sony_Music_Entertainment', '/wiki/Sony_Music_Entertainment_Inc.', '/wiki/Sony_International']
WARN_WIKI_URLS = ['/wiki/Warner_Music_Group', '/wiki/Warner_Bros.', '/wiki/WarnerMedia', '/wiki/Warner_Music', '/wiki/WEA_International', '/wiki/Warner_Brothers_Music']
INDI_WIKI_URL = '/wiki/Independent_record_label'

WIKIPEDIA_BASE_URL = 'https://en.wikipedia.org'

DEBUG = False
STOP_AT = None
MAX_DEPTH = 6
SAVE_AFTER = 100
MAX_LOOKUP_TIME_S = 600

# Set language to english as there are more infoboxes on label pages in the english wiki version
wikipedia.set_lang('en')
LABEL_MAP = pd.DataFrame()

class Classification(Enum):
    WIKI_TRY_FLAG = WIKI_TRY_FLAG
    WIKI_NO_URL_FLAG = WIKI_NO_URL_FLAG
    WIKI_DE_FLAG = WIKI_DE_FLAG
    WIKI_MAX_DEPTH = WIKI_MAX_DEPTH
    WIKI_FAIL_FLAG = WIKI_FAIL_FLAG

    FINAL_UNIV = FINAL_UNIV
    FINAL_SONY = FINAL_SONY
    FINAL_WARN = FINAL_WARN
    FINAL_INDI = FINAL_INDI
    FINAL_UNKN = FINAL_UNKN


# this class captures all relevant information of a wikipedia page
class WikipediaEntry:
    def __init__(self, shortcut=Classification.WIKI_TRY_FLAG):
        self.shortcut = shortcut
    # the type of the wikipedia entry (see enum EntryType)
    shortcut = None
    # the url is the identifier of the page
    url = None
    # for a record label: a list of parent companies
    parent_companies = []
    # for a record label: a list of distributors
    distributors = []
    # for a band/artist: a list of labels
    labels = []
    # keyword collection of 4 targets when no simpler classification happened
    keywords = {}
    # boolean if entry or descendant contains link to wiki/indi page
    contains_indi = False


def run_crawler(input_map=INPUT_LABEL_MAP, index_from=-1):
    global LABEL_MAP

    if DEBUG: print('loading label map', input_map)
    if input_map == INPUT_LABEL_MAP:
        LABEL_MAP = pd.read_csv(input_map, dtype={DISCOGS_WIKI_URL: str})
        LABEL_MAP[CLASS_WIKIPEDIA] = LABEL_MAP[CLASS_DISCOGS]
        LABEL_MAP[[WIKI_URL, WIKI_HAS_INDI_LINK]] = [np.nan, False]
        LABEL_MAP[[WIKI_KEYWORD_UNIV_SUM, WIKI_KEYWORD_SONY_SUM, WIKI_KEYWORD_WARN_SUM, WIKI_KEYWORD_INDI_SUM]] = [0, 0, 0, 0]
    else:
        LABEL_MAP = pd.read_csv(input_map, dtype={DISCOGS_WIKI_URL: str, WIKI_URL: str, WIKI_HAS_INDI_LINK: bool})

    for index, entry in tqdm(LABEL_MAP.iterrows(), total=LABEL_MAP.shape[0]):
    # for index, entry in LABEL_MAP.iterrows():
        if STOP_AT is not None and entry[RECORD_LABEL_LOW] != STOP_AT:
            continue

        if index > index_from and entry[CLASS_WIKIPEDIA] not in FINALS and entry[CLASS_WIKIPEDIA] not in WIKI_FLAGS:
        #if True:
            if DEBUG: print('------------------')
            if DEBUG: print('start lookup for: ', entry[RECORD_LABEL_LOW], f'({entry.occurrences}occ) -> ', entry[CLASS_WIKIPEDIA])
            wikipedia_entry = get_major_label_classification(entry[RECORD_LABEL_LOW], entry[DISCOGS_WIKI_URL])
            LABEL_MAP.loc[index, CLASS_WIKIPEDIA] = wikipedia_entry.shortcut
            LABEL_MAP.loc[index, WIKI_URL] = wikipedia_entry.url

            # aggregate over keywords: keywords of each wiki entry are not touched
            keyword_aggregate = aggregate_keywords(wikipedia_entry)
            LABEL_MAP.loc[index, [WIKI_KEYWORD_UNIV_SUM]] = keyword_aggregate['universal'] if 'universal' in keyword_aggregate.keys() else 0
            LABEL_MAP.loc[index, [WIKI_KEYWORD_SONY_SUM]] = keyword_aggregate['sony'] if 'sony' in keyword_aggregate.keys() else 0
            LABEL_MAP.loc[index, [WIKI_KEYWORD_WARN_SUM]] = keyword_aggregate['warner'] if 'warner' in keyword_aggregate.keys() else 0
            LABEL_MAP.loc[index, [WIKI_KEYWORD_INDI_SUM]] = keyword_aggregate['independent'] if 'independent' in keyword_aggregate.keys() else 0

            indi_entry_aggregate = aggregate_indi(wikipedia_entry)
            LABEL_MAP.loc[index, WIKI_HAS_INDI_LINK] = indi_entry_aggregate

            if DEBUG: print(entry[RECORD_LABEL_LOW], ' --> ', wikipedia_entry.shortcut, keyword_aggregate, indi_entry_aggregate)

        if index % SAVE_AFTER == 0:
            if DEBUG: print('saving at', index)
            save_archives()
            save_label_map()

    save_archives()
    save_label_map()


def get_major_label_classification(label_low, wiki_url_discogs) -> WikipediaEntry:
    global ARCHIVE_LOOKUP


    if (type(wiki_url_discogs) is float and math.isnan(wiki_url_discogs)) or not wiki_url_discogs:
        wiki_url = get_wikipedia_url(label_low)
    else:
        wiki_url = wiki_url_discogs

    # check if url was found
    if wiki_url not in WIKI_FLAGS:
        # check if lookup for url already happened
        if wiki_url not in ARCHIVE_LOOKUP.keys():
            # start new lookup when a new url is passed
            ARCHIVE_LOOKUP[wiki_url] = extract_wikipedia_page(wiki_url)
        return ARCHIVE_LOOKUP[wiki_url]

    # if a flag is being returned, create empty wikipedia entry with flag as shortcut
    else:
        return WikipediaEntry(wiki_url)


'''
This method returns the first search result of Wikipedia for a given label name
'''
def get_wikipedia_url(label_name):
    global ARCHIVE_URL_MAP

    if DEBUG: print('Get wiki url for', label_name)

    # when a known url to the label name exists, return it
    if label_name in ARCHIVE_URL_MAP.keys():
        if DEBUG: print('URL has already been searched:', ARCHIVE_URL_MAP[label_name])
        return ARCHIVE_URL_MAP[label_name]

    else:
        # remove typical fillwords from the label name and append 'records'
        search_string = clean_label(label_name)

        if DEBUG: print('search for:', search_string)
        try:
            search_res = wikipedia.search(search_string)
        except (wikipedia.exceptions.WikipediaException, requests.exceptions.ConnectionError) as e:
            if DEBUG: print('WikipediaException:', e)
            return WIKI_ERROR_FLAG_WIKI_EXCEPTION

        if DEBUG: print(search_res)
        if len(search_res) > 0:
            for current_res in search_res:
                # list results are excluded as they are no real Wikipedia articles but rather overview pages
                if 'list' not in current_res.lower():
                    if DEBUG: print('get wikipedia page for:', current_res)
                    try:
                        wiki_url = wikipedia.page(current_res).url
                        ARCHIVE_URL_MAP[label_name] = wiki_url
                        return wiki_url
                    except (requests.exceptions.ConnectionError, wikipedia.exceptions.WikipediaException, requests.exceptions.JSONDecodeError) as e:
                        if DEBUG: print('ConnectionError:', e)
                        return WIKI_ERROR_FLAG_CONNECTION
                    except wikipedia.exceptions.PageError as e:
                        if DEBUG: print('PageError:', e)
                        return WIKI_ERROR_FLAG_PAGE
                    except wikipedia.exceptions.DisambiguationError as e:
                        if DEBUG: print('DisambiguationError:', e)
                        return WIKI_ERROR_FLAG_DISAMBIGUATION
                    except wikipedia.exceptions.RedirectError as e:
                        if DEBUG: print('RedirectError:', e)
                        return WIKI_ERROR_FLAG_REDIRECT
        else:
            if DEBUG: print('no page found')
        return WIKI_NO_URL_FLAG


def clean_label(label):
    # Cleaning the label is necessary as "label_name" + "records" seemed to return in most results and first all similar
    # tokens are being removed
    fillwords = ['records', 'record', 'recording', 'recordings', 'inc', 'llc', 'licence', 'exclusive']

    label_clean = str.lower(label)
    label_clean = label_clean.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
    for fillword in fillwords:
        label_clean = label_clean.replace(fillword, '')
    return label_clean + ' records'


def extract_wikipedia_page(wiki_url, depth=0) -> WikipediaEntry:
    global ARCHIVE_LOOKUP

    # if lookup for this url already happened, return outcome
    if wiki_url in ARCHIVE_LOOKUP.keys():
        if DEBUG:
            print('return existing lookup for:', wiki_url)
        return ARCHIVE_LOOKUP[wiki_url]

    # depth of current lookup must not exceed defined max depth
    if depth > MAX_DEPTH:
        if DEBUG:
            print('Max depth reached for', wiki_url)
        return WikipediaEntry(WIKI_MAX_DEPTH)

    # else start a new lookup
    if DEBUG:
        print('start new lookup for: ', wiki_url)

    # urls on wiki pages are given without the base url but on discogs it's the full url, hence a check is needed
    if not wiki_url.startswith('https'):
        wiki_url = WIKIPEDIA_BASE_URL + wiki_url

    # create empty wikipediaEntry object
    wiki_entry = WikipediaEntry(WIKI_TRY_FLAG)
    wiki_entry.url = wiki_url
    try:
        response = requests.get(wiki_url)
    except (requests.exceptions.ConnectionError, requests.exceptions.InvalidURL, wikipedia.exceptions.WikipediaException) as e:
        if DEBUG:
            print('Connection error happened', e)
        wiki_entry.shortcut = WIKI_ERROR_FLAG_CONNECTION
        ARCHIVE_LOOKUP[wiki_url] = wiki_entry
        return wiki_entry

    soup = BeautifulSoup(response.content, 'html.parser')
    infoboxes = soup.find_all('table', {'class': 'infobox'})
    if len(infoboxes) < 1:
        if DEBUG:
            print('No infobox found, return with keyword collection')
        wiki_entry.keywords = count_keywords(soup.get_text())
        wiki_entry.shortcut = WIKI_DE_FLAG + ' (no infobox)'
        ARCHIVE_LOOKUP[wiki_url] = wiki_entry
        return wiki_entry
    else:
        infobox_rows = infoboxes[0].find_all('tr')

        # get extracted links from infobox
        wiki_entry.parent_companies = extract_infobox_links(infobox_rows, ['parent company', 'parent', 'parents'])
        wiki_entry.distributors = extract_infobox_links(infobox_rows, ['distributor(s)', 'distributors', 'distributor'])
        wiki_entry.labels = extract_infobox_links(infobox_rows, ['label', 'labels', 'label(s)'])

        # check if a parent is in the finals
        classification = compare_links_to_finals(wiki_entry.parent_companies)
        if classification is not None:
            if DEBUG: print('successful classification via parents')
            wiki_entry.shortcut = classification
        else:
            # check if distributor is in the finals
            classification = compare_links_to_finals(wiki_entry.distributors)

        if classification is not None:
            if DEBUG: print('successful classification via distributors')
            wiki_entry.shortcut = classification
        else:
            # check if a label is in the finals
            classification = compare_links_to_finals(wiki_entry.labels)

        if classification is not None:
            if DEBUG: print('successful classification via labels')
            wiki_entry.shortcut = classification

        classification_done = classification in FINALS
        if not classification_done:
            if DEBUG: print('unsuccessful classification')

            if len(wiki_entry.parent_companies) > 0:
                if DEBUG:
                    print('Start recursive lookup for parent links', wiki_entry.parent_companies)
                for parent in wiki_entry.parent_companies:
                    if parent not in ARCHIVE_LOOKUP.keys():
                        ARCHIVE_LOOKUP[parent] = extract_wikipedia_page(parent, depth + 1)
                    classification = ARCHIVE_LOOKUP[parent].shortcut
                    if classification in FINALS:
                        break

            if len(wiki_entry.distributors) > 0:
                if DEBUG:
                    print('Start recursive lookup for distributor links', wiki_entry.distributors)
                for distributor in wiki_entry.distributors:
                    if distributor not in ARCHIVE_LOOKUP.keys():
                        ARCHIVE_LOOKUP[distributor] = extract_wikipedia_page(distributor, depth + 1)
                    classification = ARCHIVE_LOOKUP[distributor].shortcut
                    if classification in FINALS:
                        break

            if len(wiki_entry.labels) > 0:
                if DEBUG:
                    print('Start recursive lookup for labels links', wiki_entry.labels)
                for label in wiki_entry.labels:
                    if label not in ARCHIVE_LOOKUP.keys():
                        ARCHIVE_LOOKUP[label] = extract_wikipedia_page(label, depth + 1)
                    classification = ARCHIVE_LOOKUP[label].shortcut
                    if classification in FINALS:
                        break

            # check if link to genereal indi wiki page exists
            for link in soup.find_all('a'):
                if INDI_WIKI_URL == link.get('href'):
                    if DEBUG: print('Contains link to wiki/indi page')
                    wiki_entry.contains_indi = True

            if DEBUG:
                print('No classification possible')
            # set classification to Dead End
            if classification is None:
                classification = WIKI_DE_FLAG + ' (lookup-end)'
        wiki_entry.shortcut = classification


    wiki_entry.keywords = count_keywords(soup.get_text())
    ARCHIVE_LOOKUP[wiki_url] = wiki_entry
    return wiki_entry

stub_hrefs = ['/wiki/Parent_company', '/wiki/Record_label', '/wiki/Digital_distribution', ]
def extract_infobox_links(rows, keywords):
    for row in rows:
        # check if it's a row with a label
        if row.findChildren('th', {'class': 'infobox-label'}):
            row_label = row.contents[0].string
            if row_label is not None and str.lower(row_label) in keywords:
                all_hrefs = [a.get('href') for a in row.find_all('a')]
                clean_hrefs = []
                # necessary for e.g. : /wiki/From_Memphis_to_Vegas_/_From_Vegas_to_Memphis
                for href in all_hrefs:
                    if href not in stub_hrefs:
                        clean_hrefs.append(href)
                return clean_hrefs
    return []


def extract_wiki_link(bs_element):
    if bs_element.find('a') is not None and bs_element.find('a').get('href') is not None:
        link = bs_element.find('a').get('href')
        if '/wiki/' in link:
            return link
    return None


def compare_links_to_finals(links):
    for link in links:
        if link in UNIV_WIKI_URLS:
            return FINAL_UNIV
        if link in WARN_WIKI_URLS:
            return FINAL_WARN
        if link in SONY_WIKI_URLS:
            return FINAL_SONY
    return None


def count_keywords(text_soup):
    collection = {
        'universal': 0,
        'sony': 0,
        'warner': 0,
        'independent': 0
    }

    for keyword in collection.keys():
        collection[keyword] = text_soup.lower().count(keyword)

    return collection


def aggregate_keywords(entry: WikipediaEntry, depth=0):
    res_collection = entry.keywords

    if depth > MAX_DEPTH:
        return res_collection
    else:
        depth += 1

    if entry.keywords:
        for parent in entry.parent_companies:
            if parent in ARCHIVE_LOOKUP.keys():
                res_collection = merge_keywords_collections(res_collection, aggregate_keywords(ARCHIVE_LOOKUP[parent], depth), depth)
        for distributor in entry.distributors:
            if distributor in ARCHIVE_LOOKUP.keys():
                res_collection = merge_keywords_collections(res_collection, aggregate_keywords(ARCHIVE_LOOKUP[distributor], depth), depth)
        for label in entry.distributors:
            if label in ARCHIVE_LOOKUP.keys():
                res_collection = merge_keywords_collections(res_collection, aggregate_keywords(ARCHIVE_LOOKUP[label], depth), depth)

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


def aggregate_indi(entry: WikipediaEntry, depth=0):
    if depth > MAX_DEPTH:
        return False

    if entry.contains_indi:
        return True
    else:
        for parent in entry.parent_companies:
            if parent in ARCHIVE_LOOKUP.keys():
                if aggregate_indi(ARCHIVE_LOOKUP[parent], depth+1):
                    return True
        for distributor in entry.distributors:
            if distributor in ARCHIVE_LOOKUP.keys():
                if aggregate_indi(ARCHIVE_LOOKUP[distributor], depth + 1):
                    return True
        for label in entry.distributors:
            if label in ARCHIVE_LOOKUP.keys():
                if aggregate_indi(ARCHIVE_LOOKUP[label], depth+1):
                    return True

    return False


def load_archives():
    global ARCHIVE_LOOKUP, ARCHIVE_URL_MAP

    try:
        with open(ARCHIVE_WIKIPEDIA_LOOKUP_PATH, 'rb+') as read_file:
            ARCHIVE_LOOKUP = pickle.load(read_file)
    except FileNotFoundError as e:
        if DEBUG: print('No archive existed at', ARCHIVE_WIKIPEDIA_LOOKUP_PATH, ', creating an empty one.')
        ARCHIVE_LOOKUP = {}
    try:
        with open(ARCHIVE_WIKIPEDIA_URL_MAP_PATH, 'rb+') as read_file:
            ARCHIVE_URL_MAP = pickle.load(read_file)
    except FileNotFoundError as e:
        if DEBUG: print('No archive existed at', ARCHIVE_WIKIPEDIA_URL_MAP_PATH, ', creating an empty one.')
        ARCHIVE_URL_MAP = {}


def save_archives():
    if DEBUG: print('saving archives')
    with open(ARCHIVE_WIKIPEDIA_LOOKUP_PATH, 'wb+') as write_file:
        pickle.dump(ARCHIVE_LOOKUP, write_file)
    with open(ARCHIVE_WIKIPEDIA_URL_MAP_PATH, 'wb+') as write_file:
        pickle.dump(ARCHIVE_URL_MAP, write_file)


def save_label_map():
    if DEBUG: print('save label map')
    LABEL_MAP.to_csv(OUTPUT_LABEL_MAP_EXT, index=False)
    LABEL_MAP[[RECORD_LABEL_LOW, 'occurrences', CLASS_TRIVIAL, CLASS_DISCOGS, CLASS_WIKIPEDIA]].to_csv(OUTPUT_LABEL_MAP, index=False)


def main(debug=None, max_depth=6, restart=False):
    global DEBUG, MAX_DEPTH

    print()
    print('##########################################################')
    print('###           CRAWLER STEP 3: WIKI CRAWLER             ###')
    print('##########################################################')

    if debug is not None:
        DEBUG = debug
    MAX_DEPTH = max_depth

    if restart:
        print('PURGING ALL WIKI FILES')
        try:
            os.remove(OUTPUT_LABEL_MAP_EXT)
        except FileNotFoundError:
            pass
        try:
            os.remove(OUTPUT_LABEL_MAP)
        except FileNotFoundError:
            pass
        try:
            os.remove(ARCHIVE_WIKIPEDIA_URL_MAP_PATH)
        except FileNotFoundError:
            pass
        try:
            os.remove(ARCHIVE_WIKIPEDIA_LOOKUP_PATH)
        except FileNotFoundError:
            pass

    load_archives()
    # generate_major_entries()
    if os.path.exists(OUTPUT_LABEL_MAP_EXT):
        run_crawler(OUTPUT_LABEL_MAP_EXT)
    else:
        run_crawler()

    return 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        save_label_map()
        save_archives()
        sys.exit(-1)
