import spotipy
import sys
import os
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
from tqdm import tqdm

from src import spotify_credentials
from src import constants

# get constants
DEBUG = constants.DEBUG

LAST_SAVE_INDEX = constants.LAST_SAVE_INDEX
SAVING_STEP = constants.SAVING_STEP
TRACK_REQUEST_BULK_SIZE = constants.ALBUM_REQUEST_BULK_SIZE
BULK_FAILED_FLAG = constants.BULK_FAILED_FLAG
FAILED_LOOKUP_FLAG = constants.FAILED_LOOKUP_FLAG

TRACK_URI = constants.TRACK_URI
ALBUM_URI = constants.ALBUM_URI
OCC = constants.OCC

INPUT_TRACK_URIS = constants.INPUT_TRACK_URIS
OUTPUT_TRACK_URIS = constants.OUTPUT_TRACK_URIS
OUTPUT_SORTED_ALBUM_URIS = constants.SORTED_ALBUM_URIS

track_uri_df = pd.DataFrame()


def run_spotify_api_lookup_for_tracks(index_from=-1):
    global track_uri_df

    spotify = get_spotipy_client()

    print('Start Spotify API lookup for albums on', INPUT_TRACK_URIS)
    track_uri_bulk = {}
    bulk_errors = 0
    for index, entry in tqdm(track_uri_df.iterrows(), total=track_uri_df.shape[0]):
        if index >= index_from and pd.isna(entry[ALBUM_URI]) and entry[ALBUM_URI] != BULK_FAILED_FLAG:
            if len(track_uri_bulk) < TRACK_REQUEST_BULK_SIZE:
                track_uri_bulk[entry[TRACK_URI]] = index
            else:
                try:
                    res = spotify.tracks(track_uri_bulk.keys())
                    if DEBUG: print('req:', track_uri_bulk.keys())
                    for track in res['tracks']:
                        # if a single album of the result is none, find all albums without low label and set bulk failed
                        # lookup flag
                        if track is None:
                            for key in track_uri_bulk.keys():

                                if pd.isna(track_uri_df.loc[track_uri_bulk[key], ALBUM_URI]):
                                    track_uri_df.loc[track_uri_bulk[key], ALBUM_URI] = BULK_FAILED_FLAG
                                    bulk_errors += 1
                        # else assign it normally
                        else:
                            track_uri = track['uri']
                            track_uri_df.loc[track_uri_bulk[track_uri], ALBUM_URI] = track['album']['uri']

                            if DEBUG:
                                print('Current album: ', track_uri,'(', track_uri_bulk[track_uri], ') --> ', track['album']['name'])
                except Exception as e:
                    if DEBUG: print('Bulk error', e)
                    for key in track_uri_bulk.keys():
                        track_uri_df.loc[track_uri_bulk[key], ALBUM_URI] = BULK_FAILED_FLAG
                    bulk_errors += 1

                track_uri_bulk.clear()
                track_uri_bulk[entry[TRACK_URI]] = index

        if index >= index_from and index % SAVING_STEP == 0:
            if DEBUG: print('saving at', index, ', bulk_errors: ', bulk_errors)
            save_album_uri_df()

    if DEBUG: print('Rerun spotify requests for failed bulk requests individually')
    lookup_errors = 0
    lookup_counter = 1
    for index, entry in tqdm(track_uri_df.iterrows(), total=track_uri_df.shape[0]):
        if index >= index_from and (entry[ALBUM_URI] == BULK_FAILED_FLAG or pd.isna(entry[ALBUM_URI])):
            lookup_counter += 1
            try:
                res = spotify.track(entry[TRACK_URI])
                track_uri_df.loc[index, ALBUM_URI] = res['album']['uri']

                if DEBUG:
                    print('Successful lookup for:', res['name'], ' --> ', res['album']['name'])
            except Exception as e:
                print('Error for', entry[TRACK_URI], ': ', e)
                track_uri_df.loc[index, ALBUM_URI] = FAILED_LOOKUP_FLAG
                lookup_errors += 1

        if index >= index_from and lookup_counter % SAVING_STEP == 0:
            if DEBUG: print('saving after', lookup_counter, ', errors: ', lookup_errors)
            save_album_uri_df()

    if DEBUG: print('final save')
    save_album_uri_df()
    create_sorted_album_uri_list()


def get_spotipy_client():
    if DEBUG:
        print('Try to create spotify client with credentials.')
    spotify = spotipy.Spotify(
        client_credentials_manager=SpotifyClientCredentials(
            client_id=spotify_credentials.CLIENT_ID,
            client_secret=spotify_credentials.CLIENT_SECRET
        )
    )
    if DEBUG:
        print('Success!')
    return spotify


def create_sorted_album_uri_list(sorted_album_uris_path=OUTPUT_SORTED_ALBUM_URIS):
    if DEBUG: print('Create sorted list of album_uris from <track_uri, album_uri> map at', sorted_album_uris_path)
    sorted_album_df = track_uri_df.drop([TRACK_URI], axis=1)
    sorted_album_df = sorted_album_df.groupby([ALBUM_URI]).aggregate({
        OCC: 'sum'
    }).reset_index()
    sorted_album_df = sorted_album_df.sort_values([OCC], ascending=False).reset_index(drop=True)
    sorted_album_df.to_csv(sorted_album_uris_path, index=False)


def save_album_uri_df(output_path=OUTPUT_TRACK_URIS):
    track_uri_df.to_csv(output_path, index=False)


def load_df():
    global track_uri_df
    if os.path.exists(OUTPUT_TRACK_URIS):
        if DEBUG: print('Reuse existing track to album uri map: ', OUTPUT_TRACK_URIS)
        track_uri_df = pd.read_csv(OUTPUT_TRACK_URIS, dtype={ALBUM_URI: str})
    else:
        if DEBUG: print('Create new track to album uri map from: ', INPUT_TRACK_URIS)
        track_uri_df = pd.read_csv(INPUT_TRACK_URIS)


def main(debug=None):
    global DEBUG

    if debug is not None:
        DEBUG = debug

    load_df()
    run_spotify_api_lookup_for_tracks()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        print('Saving current df to: ', OUTPUT_TRACK_URIS)
        save_album_uri_df()
        sys.exit(-1)

