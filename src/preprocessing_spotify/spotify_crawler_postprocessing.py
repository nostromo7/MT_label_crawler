import getopt, sys
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt

from src import constants

DEBUG = constants.DEBUG

INPUT_PATH = constants.ALBUM_URIS_WITH_LABEL_LOW
OUTPUT_PATH_LABEL_MAP = constants.LABEL_MAP
OUTPUT_PATH_COPYRIGHT_MAP = constants.COPYRIGHT_MAP

# columns for label map
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
COPYRIGHT_P = constants.COPYRIGHT_P
COPYRIGHT_C = constants.COPYRIGHT_C

# columns for filling classification of existing label map
FAILED_LOOKUP_FLAG = constants.FAILED_LOOKUP_FLAG
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR
CLASS_TRIVIAL = constants.CLASS_TRIVIAL


def create_label_map_df(existing_label_map=None):
    if DEBUG: print('Reading spotify crawler output from', INPUT_PATH)
    df_record_label_low = pd.read_csv(INPUT_PATH)

    if DEBUG: print('Creating label map')
    df_label_map = df_record_label_low.groupby(RECORD_LABEL_LOW).aggregate({
        'occurrences': 'sum'
    }).reset_index().sort_values(['occurrences'], ascending=False).reset_index(drop=True)
    df_label_map[CLASS_TRIVIAL] = np.nan

    if existing_label_map is not None:
        print('Reusing existing label map from', existing_label_map)
        df_label_map = fill_with_existing_label_classification(df_label_map, existing_label_map)

    if DEBUG: print('Saving label map to', OUTPUT_PATH_LABEL_MAP)
    df_label_map.to_csv(OUTPUT_PATH_LABEL_MAP, index=False)

    if DEBUG: print('Creating full map including copyright columns')
    df_label_map_full = df_record_label_low.groupby(RECORD_LABEL_LOW).aggregate({
        'occurrences': 'sum',
        COPYRIGHT_P: list,
        COPYRIGHT_C: list
    }).reset_index().sort_values(['occurrences'], ascending=False).reset_index(drop=True)

    if DEBUG: print('Saving copyright map to', OUTPUT_PATH_COPYRIGHT_MAP)
    df_label_map_full.to_csv(OUTPUT_PATH_COPYRIGHT_MAP, index=False)

    return df_label_map


def fill_with_existing_label_classification(label_map, existing_label_map_path):
    if DEBUG: print('fill existing classifications from:', existing_label_map_path)
    label_map_existing = pd.read_csv(existing_label_map_path)
    mapping_dict = dict(zip(label_map_existing[RECORD_LABEL_LOW], label_map_existing[RECORD_LABEL_MAJOR]))
    existing_low_level_record_labels = mapping_dict.keys()

    occ_sum = 0
    occ_flag = 0
    occ_known = 0
    occ_known_not_indi = 0
    for index, entry in tqdm(label_map.iterrows(), total=label_map.shape[0]):
        occ_sum += entry['occurrences']
        if entry[RECORD_LABEL_LOW] == FAILED_LOOKUP_FLAG:
            occ_flag += entry['occurrences']
            label_map.loc[index, CLASS_TRIVIAL] = constants.FINAL_UNKN

        elif entry[RECORD_LABEL_LOW] in existing_low_level_record_labels:
            occ_known += entry['occurrences']
            label_map.loc[index, CLASS_TRIVIAL] = mapping_dict[entry[RECORD_LABEL_LOW]]
            if mapping_dict[entry[RECORD_LABEL_LOW]] != constants.FINAL_INDI:
                occ_known_not_indi += entry['occurrences']

    if DEBUG: print('occ_sum:', occ_sum)
    if DEBUG: print('occ_flag:', occ_flag, occ_flag/occ_sum)
    if DEBUG: print('occ_known:', occ_known, occ_known/occ_sum)
    if DEBUG: print('occ_known_not_indi:', occ_known_not_indi, occ_known_not_indi/occ_sum)

    return label_map


def print_low_level_dist(df, title, max_occ):
    threhold = df.index[df['occurrences'] < 10][0]
    if DEBUG: print(df.shape, threhold)
    plt.plot(df['occurrences'])
    plt.title(title)
    plt.xlabel('Low-level record labels')
    plt.ylabel('Occurrences in dataset (log)')
    plt.yscale('log')
    plt.axvline(x=threhold, color='red', linewidth=0.5)

    plt.show()


def main(debug=None, existing_label_map=None):
    global DEBUG

    if debug is not None:
        DEBUG = debug

    create_label_map_df(existing_label_map)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
