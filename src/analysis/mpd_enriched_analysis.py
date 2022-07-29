import sys
import os
import json
from tqdm import tqdm
import pandas as pd

from src import constants
from src.utils import plot_utils

DEBUG = constants.DEBUG

PATH_TO_SLICES_ENRICHED = constants.PATH_TO_SLICES_ENRICHED

RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

COUNTING_DICT_EMPTY = {
                        FINAL_UNIV: 0,
                        FINAL_SONY: 0,
                        FINAL_WARN: 0,
                        FINAL_INDI: 0
                       }

experiment_path = '../../data/experiment_output/'
tmp_output = '../../data/experiment_output/mpd_SI'
sorted_albums_enriched = '../../data/generated/sorted_album_uris_enriched_mpd.csv'

PID = 'pid'
UNIV_SUM = 'univ_sum'
SONY_SUM = 'sony_sum'
WARN_SUM = 'warn_sum'
INDI_SUM = 'indi_sum'
COMB_SUM = 'comb_sum'

SI_THRESHOLDS = [0, 0.7, 0.8, 0.9, 1]


def create_simpson_index_file():
    all_slices = os.listdir(PATH_TO_SLICES_ENRICHED)

    count = 0

    df_list = []

    for filename in tqdm(all_slices, total=len(all_slices)):
        count += 1
        curr_data = []
        if filename.startswith('mpd.slice') and filename.endswith('.json'):
            with open(PATH_TO_SLICES_ENRICHED + filename, 'r') as read_file:
                single_slice = json.load(read_file)
                for playlist in single_slice['playlists']:
                    counting_dict = COUNTING_DICT_EMPTY.copy()
                    for track in playlist['tracks']:
                        counting_dict[track[RECORD_LABEL_MAJOR]] += 1

                    curr_data.append([
                        playlist['pid'],
                        counting_dict[FINAL_UNIV],
                        counting_dict[FINAL_SONY],
                        counting_dict[FINAL_WARN],
                        counting_dict[FINAL_INDI],
                        len(playlist['tracks'])
                    ])

                df_list.append(pd.DataFrame(curr_data, columns=[PID, UNIV_SUM, SONY_SUM, WARN_SUM, INDI_SUM, COMB_SUM]))

    df_final = pd.concat(df_list)

    df_final['univ_prob'] = df_final[UNIV_SUM] / df_final[COMB_SUM]
    df_final['sony_prob'] = df_final[SONY_SUM] / df_final[COMB_SUM]
    df_final['warn_prob'] = df_final[WARN_SUM] / df_final[COMB_SUM]
    df_final['indi_prob'] = df_final[INDI_SUM] / df_final[COMB_SUM]
    df_final['SI'] = df_final['univ_prob'] ** 2 + df_final['sony_prob'] ** 2 + df_final['warn_prob'] ** 2 + df_final['indi_prob'] ** 2

    plot_utils.bar_plot(
        input_dict={
            FINAL_UNIV: df_final[UNIV_SUM].sum(),
            FINAL_SONY: df_final[SONY_SUM].sum(),
            FINAL_WARN: df_final[WARN_SUM].sum(),
            FINAL_INDI: df_final[INDI_SUM].sum()
        },
        title='Distribution of major labels in MPD',
        colors=['lightblue', 'tomato', 'lightgreen', 'lavender', 'grey']
    )

    df_final.to_csv(tmp_output, index=False)


def run_simpson_index_analysis():
    df = pd.read_csv(tmp_output)

    print(f'Number of playlists: {df[PID].size}')
    plot_utils.simpson_index_distribution(df['SI'], title='Distribution of Simpson Indexes per playlist for MPD')

    major_count_collection = {}

    for threshold in SI_THRESHOLDS:
        df_sub = df[df['SI'] >= threshold]
        print()
        print(f' ---------- SI >= {threshold} --------------')
        print(f'Distribution of playlists with simpson index >= {threshold}')
        print(f'Number of playlists with SI >= {threshold}: {df_sub[PID].size:,}')
        print(f'Number of tracks with SI >= {threshold}: {df_sub[COMB_SUM].sum():,}')

        major_count = COUNTING_DICT_EMPTY.copy()
        major_count[FINAL_UNIV] = df_sub[UNIV_SUM].sum()
        major_count[FINAL_SONY] = df_sub[SONY_SUM].sum()
        major_count[FINAL_WARN] = df_sub[WARN_SUM].sum()
        major_count[FINAL_INDI] = df_sub[INDI_SUM].sum()
        major_count_collection[threshold] = major_count

        #run_stats_on_playlist_collection(major_count, save_collection_per_dom_major=False)

        dominant_major = 'dominant_major'
        df_sub[dominant_major] = df_sub.loc[:, [UNIV_SUM, SONY_SUM, WARN_SUM, INDI_SUM]].idxmax(axis=1)
        print(f'Avg. number of tracks per playlist where univ is dominant: {df_sub[df_sub[dominant_major] == UNIV_SUM][COMB_SUM].mean()}')
        print(f'Avg. number of tracks per playlist where sony is dominant: {df_sub[df_sub[dominant_major] == SONY_SUM][COMB_SUM].mean()}')
        print(f'Avg. number of tracks per playlist where warn is dominant: {df_sub[df_sub[dominant_major] == WARN_SUM][COMB_SUM].mean()}')
        print(f'Avg. number of tracks per playlist where indi is dominant: {df_sub[df_sub[dominant_major] == INDI_SUM][COMB_SUM].mean()}')
        print(f'Avg. number of tracks per playlist {df_sub[COMB_SUM].mean()}')

        major_count_sum = sum(major_count.values())
        print(f'Share of univ: {(major_count[FINAL_UNIV]/major_count_sum):.2%}')
        print(f'Share of sony: {(major_count[FINAL_SONY]/major_count_sum):.2%}')
        print(f'Share of warn: {(major_count[FINAL_WARN]/major_count_sum):.2%}')
        print(f'Share of indi: {(major_count[FINAL_INDI]/major_count_sum):.2%}')

    plot_utils.comparison_bar_plot(major_count_collection, title="Comparison of Simpson Index thresholds in MPD")


def run_stats_on_playlist_collection(playlist_collection, save_collection_per_dom_major=False):
    print('Total number of playlists:', len(playlist_collection))
    major_distribution = COUNTING_DICT_EMPTY.copy()
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

    count = 0
    for playlist in major_playlist_collection[FINAL_WARN]:
        if playlist['num_tracks'] > 42:
            count += 1
            print(playlist['num_tracks'], playlist['pid'], playlist['name'] , [track['track_name'] for track in playlist ['tracks']])
    print(str(count) + ' Warner playlists with length > 42')

    calculate_playlist_length(major_playlist_collection)
    if save_collection_per_dom_major:
        save_major_playlists(major_playlist_collection)


def get_most_frequent_major(playlist):
    major_distribution = COUNTING_DICT_EMPTY.copy()
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
    all_playlist_length = COUNTING_DICT_EMPTY.copy()

    n_tracks_total = 0
    n_playlists_total = 0
    for key in playlist_collection.keys():
        n_playlists = len(playlist_collection[key])
        n_tracks = 0
        for playlist in playlist_collection[key]:
            n_tracks += len(playlist['tracks'])

        n_tracks_total += n_tracks
        n_playlists_total += n_playlists

        try:
            all_playlist_length[key] = n_tracks / n_playlists
        except ZeroDivisionError:
            all_playlist_length[key] = 0

    print(f'Avg. playlist length overall: {n_tracks_total/n_playlists_total}')
    print('Avg. playlist length:', all_playlist_length)


def save_major_playlists(playlist_collection):
    for major in playlist_collection.keys():
        with open(experiment_path + major + '_SI_1', 'w') as write_file:
            output = {
                'num_playlists': len(playlist_collection[major]),
                'playlists': playlist_collection[major]
            }
            json.dump(output, write_file, indent=4)


def main():
    plot_utils.generate_histogram_of_record_label_distribution(sorted_albums_enriched, title='Top-level class distribution in sorted albums in MPD')
    create_simpson_index_file()
    run_simpson_index_analysis()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
