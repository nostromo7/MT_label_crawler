import spotipy
import sys
import os
import pandas as pd
import numpy as np
from spotipy.oauth2 import SpotifyClientCredentials
from tqdm import tqdm

from src import spotify_credentials
from src import constants

# get constants
DEBUG = constants.DEBUG

LAST_SAVE_INDEX = constants.LAST_SAVE_INDEX
SAVING_STEP = constants.SAVING_STEP
ALBUM_REQUEST_BULK_SIZE = constants.ALBUM_REQUEST_BULK_SIZE
BULK_FAILED_FLAG = constants.BULK_FAILED_FLAG
FAILED_LOOKUP_FLAG = constants.FAILED_LOOKUP_FLAG

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
COPYRIGHT_P = constants.COPYRIGHT_P
COPYRIGHT_C = constants.COPYRIGHT_C

SORTED_ALBUM_URIS = constants.SORTED_ALBUM_URIS
ALBUM_URIS_WITH_LABEL_LOW = constants.ALBUM_URIS_WITH_LABEL_LOW
PATH_TO_SLICES_ORIGINAL = constants.PATH_TO_SLICES_ORIGINAL

album_uri_df = pd.DataFrame()


def run_spotify_api_lookup(album_uris=ALBUM_URIS_WITH_LABEL_LOW, index_from=LAST_SAVE_INDEX):
    global album_uri_df

    print('Start Spotify API lookup for record labels on', album_uris)
    spotify = get_spotipy_client()
    album_uri_df = pd.read_csv(album_uris)
    if RECORD_LABEL_LOW not in album_uri_df.columns:
        album_uri_df[[RECORD_LABEL_LOW, COPYRIGHT_P, COPYRIGHT_C]] = [np.nan, np.nan, np.nan]

    if DEBUG: print('Run spotify requests in bulks of ', ALBUM_REQUEST_BULK_SIZE)
    album_uri_bulk = {}
    bulk_errors = 0
    for index, entry in tqdm(album_uri_df.iterrows(), total=album_uri_df.shape[0]):
        if index >= index_from and pd.isna(entry[RECORD_LABEL_LOW]) and entry[RECORD_LABEL_LOW] != BULK_FAILED_FLAG:
            if len(album_uri_bulk) < ALBUM_REQUEST_BULK_SIZE:
                album_uri_bulk[entry['album_uri']] = index
            else:
                try:
                    res = spotify.albums(album_uri_bulk.keys())
                    for album in res['albums']:
                        # if a single album of the result is none, find all albums without low label and set bulk failed
                        # lookup flag
                        if album is None:
                            for key in album_uri_bulk.keys():

                                if pd.isna(album_uri_df.loc[album_uri_bulk[key], RECORD_LABEL_LOW]):
                                    album_uri_df.loc[album_uri_bulk[key], RECORD_LABEL_LOW] = BULK_FAILED_FLAG
                                    bulk_errors += 1
                        # else assign it normally
                        else:
                            uri = 'spotify:album:' + album['id']
                            album_uri_df.loc[album_uri_bulk[uri], [RECORD_LABEL_LOW,
                                                                   COPYRIGHT_P,
                                                                   COPYRIGHT_C,
                                                                   'artist_name']] = format_new_entry(album)

                            if DEBUG:
                                print('Current album: ', album['name'], ' --> ', album['label'])
                except Exception as e:
                    if DEBUG: print('Bulk error', e)
                    for key in album_uri_bulk.keys():
                        album_uri_df.loc[album_uri_bulk[key], RECORD_LABEL_LOW] = BULK_FAILED_FLAG
                    bulk_errors += 1

                album_uri_bulk.clear()
                album_uri_bulk[entry['album_uri']] = index

        if index > LAST_SAVE_INDEX and index % SAVING_STEP == 0:
            if DEBUG: print('saving at', index, ', bulk_errors: ', bulk_errors)
            save_album_uri_df(ALBUM_URIS_WITH_LABEL_LOW)

    if DEBUG: print('Rerun spotify requests for failed bulk requests individually')
    lookup_errors = 0
    lookup_counter = 1
    for index, entry in tqdm(album_uri_df.iterrows(), total=album_uri_df.shape[0]):
        if index >= index_from and (entry[RECORD_LABEL_LOW] == BULK_FAILED_FLAG or pd.isna(entry[RECORD_LABEL_LOW])):
            lookup_counter += 1
            try:
                res = spotify.album(entry['album_uri'])
                album_uri_df.loc[index, [RECORD_LABEL_LOW,
                                         COPYRIGHT_P,
                                         COPYRIGHT_C,
                                         'artist_name']] = format_new_entry(res)

                if DEBUG: print('Successful lookup for:', entry['album_uri'], f'({entry.occurrences:,} os)', ' --> ', res['label'])
            except Exception as e:
                if DEBUG: print('Error for', entry['album_uri'], ': ', e)
                album_uri_df.loc[index, RECORD_LABEL_LOW] = FAILED_LOOKUP_FLAG
                lookup_errors += 1

        if index >= index_from and lookup_counter % SAVING_STEP == 0:
            print('saving after', lookup_counter, ', errors: ', lookup_errors)
            save_album_uri_df(ALBUM_URIS_WITH_LABEL_LOW)

    if DEBUG: print('Replace \'None|-\' with \'Unknown\'')
    album_uri_df.loc[album_uri_df[RECORD_LABEL_LOW].str.fullmatch('-|None', case=False, na=False), RECORD_LABEL_LOW] = 'Unknown'
    print('final save')
    save_album_uri_df(ALBUM_URIS_WITH_LABEL_LOW)


def format_new_entry(album):
    # the artist name is being processed as there are sometimes multiple artist names in the original dataset, this step
    # helps to get a more general artist name for each album but this is only for readability and has no other purpose
    artist_name = ', '.join(set([a['name'] for a in album['artists']]))
    copyright_p = np.nan
    copyright_c = np.nan
    # preprocess the copyright information by removing copyright signs
    for cr in album['copyrights']:
        if cr['type'] == 'P':
            copyright_p = cr['text'].replace('℗', '')
        if cr['type'] == 'C':
            copyright_c = cr['text'].replace('©', '')

    return [album['label'], copyright_p, copyright_c, artist_name]


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


def save_album_uri_df(output_path=ALBUM_URIS_WITH_LABEL_LOW):
    album_uri_df.to_csv(output_path, index=False)


def main(debug=None):
    global DEBUG

    if debug is not None:
        DEBUG = debug

    if os.path.exists(ALBUM_URIS_WITH_LABEL_LOW):
        # if an output file exists, reuse it
        run_spotify_api_lookup()
    else:
        # create new one from sorted list of album uris otherwise
        run_spotify_api_lookup(SORTED_ALBUM_URIS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        print('Saving current df to: ', ALBUM_URIS_WITH_LABEL_LOW)
        save_album_uri_df(ALBUM_URIS_WITH_LABEL_LOW)
        sys.exit(-1)

