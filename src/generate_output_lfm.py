import pandas
import spotipy
import sys
import os
import pandas as pd
import numpy as np
from spotipy.oauth2 import SpotifyClientCredentials
from tqdm import tqdm

import spotify_credentials
import constants

# get constants
DEBUG = constants.DEBUG

PATH_TO_LFM_ORIGINAL = constants.PATH_TO_LFM_ORIGINAL
PATH_TO_LFM_ENRICHED = constants.PATH_TO_LFM_ENRICHED

TRACK_URIS_ENRICHED = constants.TRACK_URIS_ENRICHED

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR
ALBUM_URI = constants.ALBUM_URI
TRACK_URI = constants.TRACK_URI

FINAL_INDI = constants.FINAL_INDI
FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_UNKN = constants.FINAL_UNKN


def write_label_info_back():

    label_map_df = pd.read_csv(TRACK_URIS_ENRICHED)[[TRACK_URI, RECORD_LABEL_LOW, RECORD_LABEL_MAJOR]]
    label_map_df[TRACK_URI] = label_map_df[TRACK_URI].map(lambda x: x.strip('spotify:track:'))
    lfm_df = pd.read_csv(PATH_TO_LFM_ORIGINAL, sep='\t').rename(columns={'uri': TRACK_URI})
    print(lfm_df.shape)
    print(lfm_df.columns, label_map_df.columns)
    lfm_enriched = lfm_df.merge(label_map_df, on=[TRACK_URI], how='left')
    lfm_enriched = lfm_enriched.fillna(FINAL_INDI)
    print(lfm_enriched.shape)
    lfm_enriched = lfm_enriched.rename(columns={TRACK_URI: 'uri'})
    lfm_enriched.to_csv(PATH_TO_LFM_ENRICHED, sep='\t', index=False)

    counter_dict = {
        FINAL_UNIV: 0,
        FINAL_WARN: 0,
        FINAL_INDI: 0,
        FINAL_SONY: 0,
        FINAL_UNKN: 0
    }
    counter_all = 0
    for index, entry in tqdm(lfm_enriched.iterrows(), total=len(lfm_enriched)):
        counter_all += 1
        counter_dict[entry[RECORD_LABEL_MAJOR]] += 1

    print(counter_dict)
    print(counter_all)

    for key in counter_dict.keys():
        print(f'{key}: {counter_dict[key] / counter_all:.2%}')


def main():
    write_label_info_back()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)

