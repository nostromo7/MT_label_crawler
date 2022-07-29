import math
import operator
import sys
from collections import OrderedDict

import pandas as pd
from gensim import corpora
from gensim.utils import simple_preprocess
from nltk.corpus import stopwords
from tqdm import tqdm

from src import constants

DEBUG = constants.DEBUG

INPUT_PATH = constants.LABEL_MAP_INTERIM
OUTPUT_PATH = constants.LABEL_MAP_COPYRIGHT
OUTPUT_PATH_TMP = constants.LABEL_MAP_COPYRIGHT_INTERIM
COPYRIGHT_MAP = constants.COPYRIGHT_MAP

CLASS_INTERIM = constants.CLASS_INTERIM
CLASS_COPYRIGHT = constants.CLASS_COPYRIGHT
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
COPYRIGHT_P = constants.COPYRIGHT_P
COPYRIGHT_C = constants.COPYRIGHT_C
COPYRIGHT_BOTH = 'copyright'

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]

COPYRIGHT_ALIAS_DICT = {
    FINAL_UNIV: ['universal', 'umg', 'capitol', 'emi', 'decca', 'concord', 'geffen', 'republic', 'interscope', 'motown', 'verve'],
    FINAL_WARN: ['warner', 'atlantic', 'rhino', 'wea', 'parlophone', 'spinninrecords', 'elektra'],
    FINAL_SONY: ['sony', 'bmg', 'ultra', 'rca', 'columbia', 'epic'],
    FINAL_INDI: ['independent', 'indi']
}

STOP_WORDS = []


def copyright_analysis():
    global STOP_WORDS
    STOP_WORDS = stopwords.words('english')
    STOP_WORDS.extend(['nan', 'record', 'llc', 'records', 'ltd', 'records', 'inc', 'exclusive',
                       'license', 'music', 'entertainment', 'production', 'digital', 'recordings',
                       'copyright', 'control', 'publishing', 'limited', 'recording', 'global', 'licence',
                       'corporation', 'manufactured', 'marketed', 'international', 'division', 'reserved',
                       'group', 'enterprises', 'enterprise', 'gmbh', 'productions', 'company', 'media',
                       'compilation', 'rights', 'corp', 'sound', 'distributed', 'operations', 'released',
                       'label', 'originally', 'new'])

    RELOAD = True
    if RELOAD:
        df = pd.read_csv(INPUT_PATH)
        df_copyright = pd.read_csv(COPYRIGHT_MAP)
        df = df.merge(df_copyright, on=[RECORD_LABEL_LOW, 'occurrences'], how='left')
        df[COPYRIGHT_P] = df[COPYRIGHT_P].apply(utils_preprocess_text)
        df[COPYRIGHT_C] = df[COPYRIGHT_C].apply(utils_preprocess_text)
        df[COPYRIGHT_BOTH] = df[[COPYRIGHT_P, COPYRIGHT_C]].apply(combine_copyrights, axis=1)

        df.to_csv(OUTPUT_PATH_TMP, index=False)
    else:
        df = pd.read_pickle(OUTPUT_PATH_TMP)

    bow_univ = []
    bow_warn = []
    bow_sony = []
    bow_indi = []
    bow_all = []

    df[CLASS_COPYRIGHT] = df[CLASS_INTERIM]

    n_change_sum = 0
    occ_change_sum = 0

    for index, entry in tqdm(df.iterrows(), total=df.shape[0]):
        bow_all.append(entry[COPYRIGHT_BOTH])
        if entry[CLASS_INTERIM] == FINAL_UNIV:
            bow_univ.append(entry[COPYRIGHT_BOTH])
        elif entry[CLASS_INTERIM] == FINAL_WARN:
            bow_warn.append(entry[COPYRIGHT_BOTH])
        elif entry[CLASS_INTERIM] == FINAL_SONY:
            bow_sony.append(entry[COPYRIGHT_BOTH])
        elif entry[CLASS_INTERIM] == FINAL_INDI:
            bow_indi.append(entry[COPYRIGHT_BOTH])

        if entry[CLASS_COPYRIGHT] in FINALS:
            different_major = copyright_differs_from_major(entry[RECORD_LABEL_LOW], entry[CLASS_COPYRIGHT], entry[COPYRIGHT_BOTH], entry['occurrences'])
            if different_major is not None:
                n_change_sum += 1
                occ_change_sum += entry['occurrences']
                df.loc[index, CLASS_COPYRIGHT] = different_major

        if entry[CLASS_COPYRIGHT] not in FINALS:
            df.loc[index, CLASS_COPYRIGHT] = classify_copyright(entry[COPYRIGHT_BOTH])

    if DEBUG: print('CHANGES', n_change_sum)
    if DEBUG: print('CHANGES_OCC', occ_change_sum)

    corpora.Dictionary(bow_univ)
    corpora.Dictionary(bow_warn)
    corpora.Dictionary(bow_sony)
    corpora.Dictionary(bow_indi)
    corpora.Dictionary(bow_all)

    univ_dict = create_sorted_dict(bow_univ)
    warn_dict = create_sorted_dict(bow_warn)
    sony_dict = create_sorted_dict(bow_sony)
    indi_dict = create_sorted_dict(bow_indi)
    all_dict = create_sorted_dict(bow_all)

    if DEBUG: print('univ: ', list(univ_dict.items())[0:100])
    if DEBUG: print('warn: ', list(warn_dict.items())[0:100])
    if DEBUG: print('sony: ', list(sony_dict.items())[0:100])
    if DEBUG: print('indi: ', list(indi_dict.items())[0:100])
    if DEBUG: print('all: ', list(all_dict.items())[0:100])

    df.drop([COPYRIGHT_P, COPYRIGHT_C, COPYRIGHT_BOTH], axis=1).to_csv(OUTPUT_PATH, index=False)


def utils_preprocess_text(text):
    # This is necessary for 'Unknown' entries, which have 'nan' as copyright
    if isinstance(text, float) and math.isnan(text):
        return []
    text = simple_preprocess(text)
    text = [word for word in text if (word not in STOP_WORDS and len(word) > 2)]
    return text


def combine_copyrights(copyright_p_and_c):
    copyright_p = copyright_p_and_c[0]
    copyright_c = copyright_p_and_c[1]
    copyright_original = copyright_p if len(copyright_p) > len(copyright_c) else copyright_c
    copyright_res = copyright_original
    for token in copyright_p:
        if token not in copyright_p:
            copyright_res.append(token)

    return list(copyright_res)


def remove_stopwords(copyrights):
    return [[token for token in simple_preprocess(str(cr)) if token not in STOP_WORDS] for cr in copyrights]


def tokenize_copyright(copyrights):
    for cr in copyrights:
        yield simple_preprocess(cr, deacc=True)


def analyze_copyright_p_c(df):
    n = len(df)
    n_p = len(df.loc[df[COPYRIGHT_P] != '[]'])
    n_c = len(df.loc[df[COPYRIGHT_C] != '[]'])
    n_both = len(df.loc[df[COPYRIGHT_BOTH] != '[]'])
    if DEBUG: print(f'# of labels:            {n:,}')
    if DEBUG: print(f'# of copyright_p:       {n_p:,}')
    if DEBUG: print(f'# of copyright_c:       {n_c:,}')
    if DEBUG: print(f'# of copyright_both:    {n_both:,}')


def copyright_differs(label_low, copyright_lst):
    if len(copyright_lst) == 0:
        return False
    ordered = create_sorted_dict(copyright_lst, False)

    chosen_final = None
    token_old = None
    multi_final = False
    for token in ordered.keys():
        for final in COPYRIGHT_ALIAS_DICT.keys():
            if token in COPYRIGHT_ALIAS_DICT[final]:
                if chosen_final is None:
                    chosen_final = final
                    token_old = token
                elif chosen_final == final:
                    pass
                else:
                    if DEBUG: print('For label: ', label_low, '; From', chosen_final, ' to -> ', final)
                    if DEBUG: print(f'prev. token {token_old} ({ordered[token_old]:,}) -> new token {token} ({ordered[token]:,})')
                    return True
    return multi_final


def copyright_differs_from_major(label_low, label_major, copyright_lst, occurrences):
    if len(copyright_lst) == 0:
        return None
    ordered = create_sorted_dict(copyright_lst, False)

    for token in ordered.keys():
        for final in COPYRIGHT_ALIAS_DICT.keys():
            if token in COPYRIGHT_ALIAS_DICT[final]:
                if final == label_major:
                    return None
                if final != label_major:
                    if DEBUG: print('For ', label_low, ' (', label_major, ') --> ', final, '; occ: ', occurrences)
                    if DEBUG: print(token, ': ', ordered[token])

                    return final


def create_sorted_dict(bow, flatten=True):
    global STOP_WORDS
    if flatten:
        bow = [token for sublist in bow for token in sublist]
    res = {}
    for token in bow:
        if token not in STOP_WORDS:
            if token not in res.keys():
                res[token] = 1
            else:
                res[token] += 1
    return OrderedDict(sorted(res.items(), key=operator.itemgetter(1), reverse=True))


def classify_copyright(copyright_lst) -> str:
    if len(copyright_lst) == 0:
        return FINAL_UNKN
    ordered = create_sorted_dict(copyright_lst, False)
    for token in ordered.keys():
        for final in COPYRIGHT_ALIAS_DICT.keys():
            if token in COPYRIGHT_ALIAS_DICT[final]:
                return final
    return FINAL_UNKN


def main(debug=None):
    global DEBUG

    print()
    print('##########################################################')
    print('###        CRAWLER STEP 5: COPYRIGHT CLASSIFIER        ###')
    print('##########################################################')

    if debug is not None:
        DEBUG = debug

    pd.set_option('display.width', 2000)
    pd.set_option('display.max_columns', 5)

    copyright_analysis()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
