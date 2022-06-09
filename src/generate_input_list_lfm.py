import sys
import pandas as pd
import numpy as np

from src import constants

# get constants
TRACK_URI = constants.TRACK_URI
OCC = constants.OCC

INPUT_PATH = constants.PATH_TO_LFM_ORIGINAL
OUTPUT_PATH = constants.INPUT_TRACK_URIS


def create_track_uri_list(output_path=OUTPUT_PATH):

    print('Create new list of track_uris from: ', INPUT_PATH)
    track_uri_df = pd.read_csv(INPUT_PATH, sep='\t').drop(['track_id'], axis=1)
    # TODO: REMOVE length restriction
    # track_uri_df = track_uri_df[:50]
    track_uri_df[OCC] = 1
    track_uri_df[TRACK_URI] = 'spotify:track:' + track_uri_df['uri'].astype(str)
    track_uri_df = track_uri_df.drop(['uri'], axis=1)

    track_uri_df = track_uri_df.groupby(TRACK_URI).aggregate({
        OCC: 'sum'
    }).reset_index().sort_values([OCC], ascending=False).reset_index(drop=True)

    track_uri_df = track_uri_df.assign(album_uri=np.nan)
    track_uri_df.to_csv(output_path, index=False)


def main():
    create_track_uri_list()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)

