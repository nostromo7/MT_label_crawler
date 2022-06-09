import sys
import pandas as pd
import numpy as np
from tqdm import tqdm

from src import constants

DEBUG = constants.DEBUG

INPUT_PATH = constants.LABEL_MAP_WIKIPEDIA_EXT
OUTPUT_PATH = constants.LABEL_MAP_INTERIM

# columns for label map
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
CLASS_TRIVIAL = constants.CLASS_TRIVIAL

CLASS_DISCOGS = constants.CLASS_DISCOGS
DISCOGS_KEYWORD_UNIV_SUM = constants.DISCOGS_KEYWORD_UNIV_SUM
DISCOGS_KEYWORD_SONY_SUM = constants.DISCOGS_KEYWORD_SONY_SUM
DISCOGS_KEYWORD_WARN_SUM = constants.DISCOGS_KEYWORD_WARN_SUM
DISCOGS_KEYWORD_INDI_SUM = constants.DISCOGS_KEYWORD_INDI_SUM
DISCOGS_KEYWORDS = [DISCOGS_KEYWORD_UNIV_SUM, DISCOGS_KEYWORD_SONY_SUM, DISCOGS_KEYWORD_WARN_SUM,
                    DISCOGS_KEYWORD_INDI_SUM]

CLASS_WIKIPEDIA = constants.CLASS_WIKIPEDIA
WIKI_HAS_INDI_LINK = constants.WIKI_HAS_INDI_LINK
WIKI_KEYWORD_UNIV_SUM = constants.WIKI_KEYWORD_UNIV_SUM
WIKI_KEYWORD_SONY_SUM = constants.WIKI_KEYWORD_SONY_SUM
WIKI_KEYWORD_WARN_SUM = constants.WIKI_KEYWORD_WARN_SUM
WIKI_KEYWORD_INDI_SUM = constants.WIKI_KEYWORD_INDI_SUM
WIKI_KEYWORDS = [WIKI_KEYWORD_UNIV_SUM, WIKI_KEYWORD_SONY_SUM, WIKI_KEYWORD_WARN_SUM,
                 WIKI_KEYWORD_INDI_SUM]

CLASS_INTERIM = constants.CLASS_INTERIM
EXT_COLUMNS = [WIKI_HAS_INDI_LINK] + WIKI_KEYWORDS + DISCOGS_KEYWORDS

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]

WIKI_KEYWORD_TO_FINAL_MAP = {
    WIKI_KEYWORD_UNIV_SUM: FINAL_UNIV,
    WIKI_KEYWORD_SONY_SUM: FINAL_SONY,
    WIKI_KEYWORD_WARN_SUM: FINAL_WARN,
    WIKI_KEYWORD_INDI_SUM: FINAL_INDI
}


def run_trivial_label_mapping(wiki_keyword_agg_under=2, wiki_keyword_agg_over=0.25):
    if DEBUG: print('Read label map from', INPUT_PATH)
    df_label_map = pd.read_csv(INPUT_PATH,
                               dtype={WIKI_HAS_INDI_LINK: bool},
                               usecols=([RECORD_LABEL_LOW, 'occurrences', CLASS_TRIVIAL, CLASS_DISCOGS,
                                         CLASS_WIKIPEDIA] + EXT_COLUMNS))
    if DEBUG: print(df_label_map.columns)
    df_label_map[CLASS_INTERIM] = np.nan

    df_label_map.loc[~df_label_map[CLASS_INTERIM].isin(FINALS), CLASS_INTERIM] = df_label_map.loc[df_label_map[CLASS_WIKIPEDIA].isin(FINALS), CLASS_WIKIPEDIA]

    num = 0
    occ_sum = 0
    for index, entry in tqdm(df_label_map.iterrows(), total=df_label_map.shape[0]):
        if entry[CLASS_INTERIM] not in FINALS:

            # Classify label as INDI if sum of Wiki keyword aggregate is under threshold (e.g. 2)
            if entry[WIKI_HAS_INDI_LINK] is True and sum_of_keywords_under(entry, WIKI_KEYWORDS, wiki_keyword_agg_under):
                num += 1
                occ_sum += entry['occurrences']
                df_label_map.loc[index, CLASS_INTERIM] = FINAL_INDI
            # If sum of wiki keyword aggregate is bigger than threshold (e.g. 0.25) take max as major label
            elif not sum_of_keywords_under(entry, WIKI_KEYWORDS, wiki_keyword_agg_over):
                num += 1
                occ_sum += entry['occurrences']
                df_label_map.loc[index, CLASS_INTERIM] = get_max_target(entry, WIKI_KEYWORDS)

    if DEBUG: print(num, ' changes: (', occ_sum, ' occ)')

    if DEBUG: print('Save interim mapping output to', OUTPUT_PATH)
    df_label_map.drop(EXT_COLUMNS, axis=1).to_csv(OUTPUT_PATH, index=False)
    df_label_map[([RECORD_LABEL_LOW, 'occurrences', CLASS_WIKIPEDIA, WIKI_HAS_INDI_LINK] + WIKI_KEYWORDS)].to_csv(OUTPUT_PATH.replace('.csv', '_ext.csv'), index=False)


def sum_of_keywords_under(entry, columns, threshold):
    keyword_sum = 0
    for column in columns:
        keyword_sum += entry[column]
    return keyword_sum <= threshold


def get_max_target(entry, columns):
    label = None
    prev_max = 0
    max = 0
    for column in columns:
        if entry[column] >= max:
            prev_max = max
            max = entry[column]
            label = WIKI_KEYWORD_TO_FINAL_MAP[column]
    # prevents a classification if there is a tie
    if max > prev_max:
        return label
    else:
        return None


def main(debug=None, wiki_keyword_agg_under=2, wiki_keyword_agg_over=0.25):
    global DEBUG

    print()
    print('##########################################################')
    print('###           CRAWLER STEP 4: INTERIM MAPPING          ###')
    print('##########################################################')

    if debug is not None:
        DEBUG = debug

    pd.set_option('display.width', 2000)
    pd.set_option('display.max_columns', 5)
    run_trivial_label_mapping(wiki_keyword_agg_under, wiki_keyword_agg_over)
    return 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
