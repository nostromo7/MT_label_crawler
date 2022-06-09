import sys
import os
import json
import time
import pandas as pd
from tqdm import tqdm

import constants

'''
This script is mapping low-level record labels and the final major label classification back into the MPD dataset. 
'''

DEBUG = constants.DEBUG

PATH_TO_SLICES_ORIGINAL = constants.PATH_TO_SLICES_ORIGINAL
PATH_TO_SLICES_ENRICHED = constants.PATH_TO_SLICES_ENRICHED

ALBUM_URIS_ENRICHED = constants.ALBUM_URIS_ENRICHED

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR
ALBUM_URI = constants.ALBUM_URI

FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN


def write_label_info_back():
    df_record_labels = pd.read_csv(ALBUM_URIS_ENRICHED)

    record_label_low_dict = pd.Series(df_record_labels[RECORD_LABEL_LOW].values, index=df_record_labels[ALBUM_URI]).to_dict()
    record_label_major_dict = pd.Series(df_record_labels[RECORD_LABEL_MAJOR].values, index=df_record_labels[ALBUM_URI]).to_dict()
    start = time.time()
    count = 1
    all_slices = os.listdir(PATH_TO_SLICES_ORIGINAL)

    for filename in tqdm(all_slices, total=len(all_slices)):
        if filename.startswith('mpd.slice') and filename.endswith('.json'):
            with open(PATH_TO_SLICES_ORIGINAL + filename, 'r') as read_file:
                tmp = time.time()
                if DEBUG:
                    print('Current File: ' + filename + ', ' + str(count) + f'/1000 ({(tmp - start):.2}s)')
                count += 1
                single_slice = json.load(read_file)
                for playlist in single_slice['playlists']:
                    for track in playlist['tracks']:
                        try:
                            record_label_low = record_label_low_dict[track['album_uri']]
                            record_label_low = record_label_low if record_label_low != FINAL_UNKN else FINAL_INDI
                            track['record_label_low'] = record_label_low
                        except KeyError:
                            track['record_label_low'] = FINAL_UNKN
                        try:
                            record_label_major = record_label_major_dict[track['album_uri']]
                            record_label_major = record_label_major if record_label_major != FINAL_UNKN else FINAL_INDI
                            track['record_label_major'] = record_label_major
                        except KeyError:
                            track['record_label_major'] = FINAL_UNKN

            with open(PATH_TO_SLICES_ENRICHED + filename, 'w') as write_file:
                json.dump(single_slice, write_file, indent=4)


def main():
    write_label_info_back()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)

