import pandas as pd
import pickle

from src import constants
from src.preprocessing_spotify import spotify_record_label_crawler as scm
from src.label_crawler.discogs_label_crawler import DiscogsEntry

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 8)

LABEL_MAP = constants.LABEL_MAP
ALBUM_URIS_WITH_LABEL_LOW = constants.ALBUM_URIS_WITH_LABEL_LOW
ARCHIVE_LOOKUP_PATH = constants.ARCHIVE_DISCOGS_LOOKUP_PATH
ARCHIVE_DISCOGS_ID_MAP_PATH = constants.ARCHIVE_DISCOGS_ID_MAP_PATH

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW

LABEL_MAP = pd.read_csv(LABEL_MAP)
album_uri_map = pd.read_csv(ALBUM_URIS_WITH_LABEL_LOW)
df = album_uri_map.merge(LABEL_MAP, on=[RECORD_LABEL_LOW], how='left')

with open(ARCHIVE_LOOKUP_PATH, 'rb+') as read_file:
    discogs_archive = pickle.load(read_file)
with open(ARCHIVE_DISCOGS_ID_MAP_PATH, 'rb+') as read_file:
    discogs_id_archive = pickle.load(read_file)

print('Lookup utils')
mode = ''
while str.lower(mode) != 'exit':
    print('What do you want to search for?')
    print('[U] album_uri (lookup)')
    print('[A] album_name (lookup)')
    print('[N] artist_name (lookup)')
    print('[S] album_uri (spotify)')
    print('[L] label_name (lookup + discogs_archive)')
    print('[I] label_name -> label_id (discogs_archive)')
    print('[exit] back / stop')
    mode = input('')

    if str.lower(mode) == 'u':
        uri = input('Enter album_uri: ')
        while str.lower(uri) != 'exit':

            entry = df.loc[df['album_uri'] == uri]
            print('RESULT:')
            print(entry)

            uri = input('Enter album_uri: ')

    elif str.lower(mode) == 'a':
        album_name = input('Enter album_name: ')
        while str.lower(album_name) != 'exit':
            entry = df.loc[df['album_name'].str.contains(album_name, na=False)]
            print('RESULT:')
            print(entry)

            album_name = input('Enter album_name: ')

    elif str.lower(mode) == 'n':
        artist_name = input('Enter artist_name: ')
        while str.lower(artist_name) != 'exit':
            entry = df.loc[df['artist_name'].str.contains(artist_name, na=False)]
            print('RESULT:')
            print(entry)

            artist_name = input('Enter artist_name: ')

    elif str.lower(mode) == 's':
        spotify = scm.get_spotipy_client()
        uri = input('Enter album_uri: ')
        while str.lower(uri) != 'exit':
            res = spotify.album(uri)
            print(res)
            print(res['label'])

            uri = input('Enter album_uri: ')

    elif str.lower(mode) == 'l':
        label_name = input('Enter label_name: ')
        while str.lower(label_name) != 'exit':
            print('Lookup:')
            entry = df.loc[df[RECORD_LABEL_LOW].str.contains(label_name, na=False)]
            print('RESULT:')
            print(entry)
            print('Discogs archive:')
            if label_name in discogs_id_archive.keys():
                label_id = discogs_id_archive[label_name]
                print('Label_id', label_id)
                if label_id in discogs_archive.keys():
                    print(discogs_archive[label_id])
                else:
                    print(label_name, 'is not in ', ARCHIVE_LOOKUP_PATH)
            else:
                print(label_name, 'is not in ', ARCHIVE_DISCOGS_ID_MAP_PATH)

            label_name = input('Enter label_name: ')

    elif str.lower(mode) == 'i':
        label_name = input('Enter label_name: ')
        while str.lower(label_name) != 'exit':
            if label_name in discogs_id_archive.keys():
                print(discogs_id_archive[label_name])
            else:
                print(label_name, 'is not in ', ARCHIVE_DISCOGS_ID_MAP_PATH)

            label_name = input('Enter label_name: ')
    else:
        print('Unknown mode')

