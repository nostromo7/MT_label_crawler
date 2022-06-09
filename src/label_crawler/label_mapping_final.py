import pandas as pd
from tqdm import tqdm
import sys

from src import constants

DEBUG = constants.DEBUG

LABEL_MAP_COPYRIGHT = constants.LABEL_MAP_COPYRIGHT
LABEL_MAP_DISCOGS_EXT = constants.LABEL_MAP_DISCOGS_EXT
LABEL_MAP_FINAL = constants.LABEL_MAP_FINAL
LABEL_MAP_FINAL_STATS = constants.LABEL_MAP_FINAL_STATS

CLASS_COPYRIGHT = constants.CLASS_COPYRIGHT
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR

CLASS_DISCOGS = constants.CLASS_DISCOGS
DISCOGS_NO_PAR_FLAG = constants.DISCOGS_NO_PAR_FLAG
DISCOGS_KEYWORD_UNIV_SUM = constants.DISCOGS_KEYWORD_UNIV_SUM
DISCOGS_KEYWORD_SONY_SUM = constants.DISCOGS_KEYWORD_SONY_SUM
DISCOGS_KEYWORD_WARN_SUM = constants.DISCOGS_KEYWORD_WARN_SUM
DISCOGS_KEYWORD_INDI_SUM = constants.DISCOGS_KEYWORD_INDI_SUM

DISCOGS_KEYWORDS = [
    DISCOGS_KEYWORD_UNIV_SUM,
    DISCOGS_KEYWORD_SONY_SUM,
    DISCOGS_KEYWORD_WARN_SUM,
    DISCOGS_KEYWORD_INDI_SUM
]

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]

KEYWORD_TO_FINAL_MAP = {
    DISCOGS_KEYWORD_UNIV_SUM: FINAL_UNIV,
    DISCOGS_KEYWORD_SONY_SUM: FINAL_SONY,
    DISCOGS_KEYWORD_WARN_SUM: FINAL_WARN,
    DISCOGS_KEYWORD_INDI_SUM: FINAL_INDI
}

CORRECTION_DICT = {
    'glassnote': FINAL_SONY,
    'chance the rapper': FINAL_INDI,
    'domino recording co': FINAL_INDI,
    'bread winners': FINAL_WARN,
    'rise records': FINAL_INDI,
    'integrity music': FINAL_INDI,
    'monstercat': FINAL_INDI
}


def run_final_mapping(discogs_keyword_agg_over=0.2):
    df = pd.read_csv(LABEL_MAP_COPYRIGHT)
    df_discogs = pd.read_csv(LABEL_MAP_DISCOGS_EXT)

    df[RECORD_LABEL_MAJOR] = df[CLASS_COPYRIGHT]
    # Note: This is necessary as the previous step (copyright classification) possibly classifies labels with the name
    # 'Unknown' to a major because while there is no low-level record label information, there might be copyright information
    df.loc[df[RECORD_LABEL_LOW] == FINAL_UNKN, RECORD_LABEL_MAJOR] = FINAL_UNKN

    for index, entry in tqdm(df.iterrows(), total=df.shape[0]):
        correction_key = low_label_matches_correction(entry[RECORD_LABEL_LOW])
        if correction_key is not None:
            df.loc[index, RECORD_LABEL_MAJOR] = CORRECTION_DICT[correction_key]
        elif entry[RECORD_LABEL_MAJOR] not in FINALS or entry[RECORD_LABEL_MAJOR] == FINAL_UNKN:
            df.loc[index, RECORD_LABEL_MAJOR] = get_max_target(df_discogs.loc[index], DISCOGS_KEYWORDS, discogs_keyword_agg_over)

        if entry[RECORD_LABEL_MAJOR] not in FINALS or entry[RECORD_LABEL_MAJOR] == FINAL_UNKN:
            df.loc[index, RECORD_LABEL_MAJOR] = FINAL_INDI

    df.to_csv(LABEL_MAP_FINAL_STATS, index=False)
    df = df[[RECORD_LABEL_LOW, 'occurrences', RECORD_LABEL_MAJOR]]
    df.to_csv(LABEL_MAP_FINAL, index=False)


def low_label_matches_correction(low_label):
    for key in CORRECTION_DICT.keys():
        if key in low_label.lower():
            return key
    return None


def get_max_target(entry, columns, threshold):
    label = FINAL_INDI
    keyword_sum = 0
    for column in columns:
        keyword_sum += entry[column]
    if keyword_sum < threshold:
        return label
    prev_max = 0
    max = 0
    for column in columns:
        if entry[column] >= max and column != DISCOGS_KEYWORD_INDI_SUM:
            prev_max = max
            max = entry[column]
            label = KEYWORD_TO_FINAL_MAP[column]
    if max == prev_max:
        return FINAL_INDI
    else:
        return label


def main(debug=None, discogs_keyword_agg_over=0.2):
    global DEBUG

    print()
    print('##########################################################')
    print('###           CRAWLER STEP 6: FINAL MAPPING            ###')
    print('##########################################################')

    if debug is not None:
        DEBUG = debug

    pd.set_option('display.width', 2000)
    pd.set_option('display.max_columns', 5)
    run_final_mapping(discogs_keyword_agg_over)
    return 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)

