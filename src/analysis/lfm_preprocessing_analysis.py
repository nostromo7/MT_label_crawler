import pandas as pd
from tqdm import tqdm
import collections
from src import constants
from mpd_base_analysis import analyze_histogram

remove_failed_lookups_from_stats = True

INPUT_PATH = constants.PATH_TO_LFM_ORIGINAL
ALBUM_URIS_AFTER_PREPROCESSING = constants.ALBUM_URIS_WITH_LABEL_LOW

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
ALBUM_URI = constants.ALBUM_URI
OCC = constants.OCC
TRACK_URI = 'uri'
ARTIST_NAME = 'artist_name'

BULK_FAILED_FLAG = constants.BULK_FAILED_FLAG
FAILED_LOOKUP_FLAG = constants.FAILED_LOOKUP_FLAG

total_tracks = 0
tracks = set()
artists = set()
albums = set()
labels_low = set()
track_histogram = collections.Counter()
artist_histogram = collections.Counter()
album_histogram = collections.Counter()
label_low_histogram = collections.Counter()


def collect_track_info():
    global total_tracks, tracks
    print('Running track analysis')

    df_tracks = pd.read_csv(INPUT_PATH, sep='\t').drop(['track_id'], axis=1)

    for index, entry in tqdm(df_tracks.iterrows(), total=df_tracks.shape[0]):
        total_tracks += 1
        tracks.add(entry[TRACK_URI])
        track_histogram[entry[TRACK_URI]] += 1


def collect_album_and_label_info():
    print('Running album and low-level label analysis')

    df = pd.read_csv(ALBUM_URIS_AFTER_PREPROCESSING)

    for index, entry in tqdm(df.iterrows(), total=df.shape[0]):
        if remove_failed_lookups_from_stats and (entry[RECORD_LABEL_LOW] == BULK_FAILED_FLAG or entry[RECORD_LABEL_LOW] == FAILED_LOOKUP_FLAG):
            continue

        albums.add(entry[ALBUM_URI])
        artists.add(entry[ARTIST_NAME])
        labels_low.add(entry[RECORD_LABEL_LOW])

        album_histogram[entry[ALBUM_URI]] += entry[OCC]
        artist_histogram[entry[ARTIST_NAME]] += entry[OCC]
        label_low_histogram[entry[RECORD_LABEL_LOW]] += entry[OCC]


def run_analysis_output():
    print("number of tracks", total_tracks)
    print("number of unique tracks", len(tracks))
    print("number of unique albums", len(albums))
    print("number of unique artists", len(artists))
    print("number of unique low-level_labels", len(labels_low))

    print(track_histogram.most_common(50))
    analyze_histogram(track_histogram, plot_title='Distribution of sorted track occurrences in LFM-2b', plot_focus='tracks')
    analyze_histogram(album_histogram, plot_title='Distribution of sorted album occurrences in LFM-2b', plot_focus='albums')
    analyze_histogram(artist_histogram, plot_title='Distribution of sorted artist occurrences in LFM-2b', plot_focus='artists')
    analyze_histogram(label_low_histogram, plot_title='Distribution of sorted low-level label occurrences in LFM-2b', plot_focus='low-level labels')


if __name__ == "__main__":
    collect_track_info()
    collect_album_and_label_info()
    run_analysis_output()
