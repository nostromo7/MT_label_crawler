import sys
import os
import json
import time
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import numpy as np


from src import constants

DEBUG = constants.DEBUG
PATH_TO_SLICES_ENRICHED = constants.PATH_TO_SLICES_ENRICHED
ALBUM_URIS_WITH_LABEL_LOW = constants.ALBUM_URIS_WITH_LABEL_LOW
LABEL_MAP_FINAL = constants.LABEL_MAP_FINAL
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR
FINAL_UNKN = constants.FINAL_UNKN


FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI


def run_simpson_index_stats():
    all_slices = os.listdir(PATH_TO_SLICES_ENRICHED)

    simpson_index_collection = []
    high_simpson_index_collection = []
    max_simpson_index_collection = []
    count = 0
    for filename in tqdm(all_slices, total=len(all_slices)):
        count += 1
        if filename.startswith('mpd.slice') and filename.endswith('.json'):
            with open(PATH_TO_SLICES_ENRICHED + filename, 'r') as read_file:
                single_slice = json.load(read_file)
                for playlist in single_slice['playlists']:
                    counting_dict = {
                        FINAL_UNIV: 0,
                        FINAL_SONY: 0,
                        FINAL_WARN: 0,
                        FINAL_INDI: 0
                    }
                    for track in playlist['tracks']:
                        counting_dict[track[RECORD_LABEL_MAJOR]] += 1

                    n_tracks = len(playlist['tracks'])
                    univ_prob = counting_dict[FINAL_UNIV] / n_tracks
                    sony_prob = counting_dict[FINAL_SONY] / n_tracks
                    warn_prob = counting_dict[FINAL_WARN] / n_tracks
                    indi_prob = counting_dict[FINAL_INDI] / n_tracks
                    simpson_index = univ_prob ** 2 + sony_prob ** 2 + warn_prob ** 2 + indi_prob ** 2
                    simpson_index_collection.append(simpson_index)

                    if simpson_index >= 0.8:
                        high_simpson_index_collection.append(playlist)
                    if simpson_index == 1:
                        max_simpson_index_collection.append(playlist)
        if count > 5:
            pass


    print('Distribution of playlists with simpson index >= 0.8')
    run_stats_on_playlist_collection(high_simpson_index_collection)
    print()
    print('Distribution of playlists with simpson index == 1.0')
    run_stats_on_playlist_collection(max_simpson_index_collection)

    number_of_buckets = 30

    bins = np.linspace(0.2, 1, number_of_buckets)
    (n2, bins2, patches) = plt.hist(simpson_index_collection, bins, rwidth=0.85)
    plt.yscale('log')
    plt.title("Distribution of Simpson Index over all playlists")
    plt.xlabel("Simpson Index")
    plt.ylabel("Occurrences of Simpson Index")
    max = n2.max()
    max_index = bins2[np.where(n2 == max)]

    ones = n2[-1]
    ones_index = bins[-2]

    # bins2 = bins2.tolist()
    # max_index = bins2[bins2.index(max_index[0])+1]

    ax = plt.axis()
    plt.annotate(str(int(max)), xy=(max_index, max + (0.05 * max)), color='red')
    plt.annotate(str(int(ones)), xy=(ones_index, ones + (0.05 * ones)), color='red')
    plt.show()


def run_stats_on_playlist_collection(playlist_collection):
    print('Total number of playlists:', len(playlist_collection))
    major_distribution = {
        FINAL_UNIV: 0,
        FINAL_SONY: 0,
        FINAL_WARN: 0,
        FINAL_INDI: 0
    }
    major_playlist_collection = {
        FINAL_UNIV: [],
        FINAL_SONY: [],
        FINAL_WARN: [],
        FINAL_INDI: []
    }
    for playlist in playlist_collection:
        max_major = get_most_frequent_major(playlist)
        if max_major is None:
            print('Error')
            print(playlist)
            sys.exit(0)
        major_distribution[max_major] += 1
        major_playlist_collection[max_major].append(playlist)
    print('Distribution of most frequent majors:', major_distribution)
    calculate_playlist_length(major_playlist_collection)


def get_most_frequent_major(playlist):
    major_distribution = {
        FINAL_UNIV: 0,
        FINAL_SONY: 0,
        FINAL_WARN: 0,
        FINAL_INDI: 0
    }
    for track in playlist['tracks']:
        major_distribution[track[RECORD_LABEL_MAJOR]] += 1

    max_major = None
    max_value = -1
    for key in major_distribution.keys():
        if major_distribution[key] > max_value:
            max_value = major_distribution[key]
            max_major = key

    return max_major


def calculate_playlist_length(playlist_collection):
    all_playlist_length = {
        FINAL_UNIV: 0,
        FINAL_SONY: 0,
        FINAL_WARN: 0,
        FINAL_INDI: 0
    }
    for key in playlist_collection.keys():
        n_playlists = len(playlist_collection[key])
        n_tracks = 0
        for playlist in playlist_collection[key]:
            n_tracks += len(playlist['tracks'])

        try:
            all_playlist_length[key] = n_tracks / n_playlists
        except ZeroDivisionError:
            all_playlist_length[key] = 0

    print('Avg. playlist length:', all_playlist_length)


def main():
    run_simpson_index_stats()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
