import sys
import os
import json
import time
import pandas as pd
from tqdm import tqdm

from src import constants

"""" Spotify crawler preprocessing

Extracts relevant information from original data slices:
    album_uri...    unique identifier of album
    album_name...   name of album, if multiple names per uri exist, concat them
    artist_name...  name of artist, if multiple names exist per uri, concat them
    occurrences...  number of occurrences of uri in original data slices 
"""

# get constants
DEBUG = constants.DEBUG

PATH_TO_SLICES_ORIGINAL = constants.PATH_TO_SLICES_ORIGINAL

SORTED_ALBUM_URIS = constants.SORTED_ALBUM_URIS


def generate_sorted_albums_df(playlist_max=None, cols_to_select=['album_uri', 'album_name', 'artist_name']):
    print('Generate new sorted albums dataframe from MPD slices at', PATH_TO_SLICES_ORIGINAL)
    start = time.time()
    df_list = []
    count = 1
    all_slices = os.listdir(PATH_TO_SLICES_ORIGINAL)
    for filename in tqdm(all_slices, total=len(all_slices)):
        if filename.startswith('mpd.slice') and filename.endswith('.json'):
            with open(PATH_TO_SLICES_ORIGINAL + filename, 'r') as read_file:
                tmp = time.time()
                if DEBUG:
                    print('Current File: ' + filename + ', ' + str(count) + f'/1000 ({(tmp - start):.2}s)')
                count += 1
                playlist_list = []
                # make list of single playlists of slice
                for playlist in json.load(read_file)['playlists']:
                    playlist_df = pd.DataFrame(playlist['tracks'], columns=cols_to_select)
                    playlist_list.append(playlist_df)

            # concat single playlist dfs and aggregate occurrences
            file_df = pd.concat(playlist_list, axis=0)
            file_df['occurrences'] = playlist_df['album_uri']
            file_df = file_df.groupby(['album_uri']).aggregate({
                'album_name': lambda d: ', '.join(set(d)),
                'artist_name': lambda d: ', '.join(set(d)),
                'occurrences': 'size'
            }).reset_index()

            # TODO: REMOVE length restriction
            # file_df = file_df[:50]

            df_list.append(file_df)

            # TODO: REMOVE length restriction
            # break

        if count == playlist_max:
            print('reached max number of playlists: ', playlist_max)
            break

    print('Concatenating single slice dfs...')
    df = pd.concat(df_list, axis=0)
    df = df.groupby(['album_uri']).aggregate({
                        'album_name': lambda d: ', '.join(set(d)),
                        'artist_name': lambda d: ', '.join(set(d)),
                        'occurrences': 'sum'
                    }).reset_index()
    df = df.sort_values(['occurrences'], ascending=False).reset_index(drop=True)
    print('Saving...')
    df.to_csv(SORTED_ALBUM_URIS, index=False)
    end = time.time()
    print("Saved generated sorted album_uri df to: " + SORTED_ALBUM_URIS + f', ({(end - start) / 60:.2}m)')


def main():
    generate_sorted_albums_df()

    return 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)

