import pandas as pd
import numpy as np
import sys
from tqdm import tqdm

from src import constants
from src.utils import plot_utils

PATH_TO_LFM_ENRICHED = constants.PATH_TO_LFM_ENRICHED
PATH_TO_LFM_LISTENING_EVENTS = constants.PATH_TO_LFM_LISTENING_EVENTS
OUTPUT_MAJOR_COUNTS = constants.PATH_TO_LFM_LISTENING_EVENTS.replace('.tsv', '_condensed.csv')
sorted_albums_enriched = '../../data/generated/sorted_album_uris_enriched_lfm.csv'

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR

USER_ID = 'user_id'
TRACK_ID = 'track_id'
UNIV_SUM = 'univ_sum'
SONY_SUM = 'sony_sum'
WARN_SUM = 'warn_sum'
INDI_SUM = 'indi_sum'
UNKN_SUM = 'unkn_sum'
COMB_SUM = 'comb_sum'

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

COUNTING_DICT_EMPTY = {
                        FINAL_UNIV: 0,
                        FINAL_SONY: 0,
                        FINAL_WARN: 0,
                        FINAL_INDI: 0,
                        FINAL_UNKN: 0
                       }

SI_THRESHOLDS = [0, 0.7, 0.8, 0.9, 1]


def lfm_enriched_analysis():
    df = pd.read_csv(PATH_TO_LFM_ENRICHED, sep='\t')
    print(df.head())
    print(df.columns)

    n_tracks = 0
    missing = 0
    major_counting_dict = COUNTING_DICT_EMPTY.copy()

    for index, entry in tqdm(df.iterrows(), total=df.shape[0]):
        n_tracks += 1
        if entry[RECORD_LABEL_MAJOR] is not None:
            major_counting_dict[entry[RECORD_LABEL_MAJOR]] += 1
        else:
            major_counting_dict[FINAL_UNKN] += 1

    plot_utils.pie_plot(
        input_dict=major_counting_dict,
        title='Distribution of major labels in LFM-2b Spotify dataset',
        colors=['lightblue', 'tomato', 'lightgreen', 'lavender', 'grey']
    )

    plot_utils.bar_plot(
        input_dict=major_counting_dict,
        title='Distribution of major labels in LFM-2b Spotify dataset',
        colors=['lightblue', 'tomato', 'lightgreen', 'lavender', 'grey']
    )


def create_major_counts_per_user(output_path=OUTPUT_MAJOR_COUNTS, occ_threshold=3, remove_duplicates=True):
    print('Reading label map for LFM-2b from', PATH_TO_LFM_ENRICHED)
    df_label_map = pd.read_csv(PATH_TO_LFM_ENRICHED, sep='\t')[[TRACK_ID, RECORD_LABEL_MAJOR]]
    track_id_set = set(df_label_map[TRACK_ID].unique())
    record_label_major_dict = pd.Series(df_label_map[RECORD_LABEL_MAJOR].values, index=df_label_map[TRACK_ID]).to_dict()
    track_occurrences = []

    print('Start processing Listening-Event tsv from', PATH_TO_LFM_LISTENING_EVENTS)
    chunksize = 10 ** 6
    counter = 0
    df_list = []

    for chunk in pd.read_csv(PATH_TO_LFM_LISTENING_EVENTS, sep='\t', chunksize=chunksize):
        print('chunk #', counter)

        curr_data = []
        chunk_reduced = chunk[[USER_ID, TRACK_ID]]
        chunk_grouped = chunk_reduced.groupby(USER_ID)
        for user in tqdm(chunk_grouped.groups.keys(), total=len(chunk_grouped.groups.keys())):
            tracks = chunk_grouped.get_group(user)[TRACK_ID]
            counting_dict = COUNTING_DICT_EMPTY.copy()
            tracks = tracks.groupby(tracks).filter(lambda x: len(x) >= occ_threshold)
            if remove_duplicates:
                tracks = tracks.drop_duplicates()

            for track in tracks:
                if track in track_id_set:
                    counting_dict[record_label_major_dict[track]] += 1
                else:
                    counting_dict[FINAL_UNKN] += 1

            curr_data.append([
                user,
                counting_dict[FINAL_UNIV],
                counting_dict[FINAL_SONY],
                counting_dict[FINAL_WARN],
                counting_dict[FINAL_INDI],
                counting_dict[FINAL_UNKN],
                len(tracks)
            ])

        df_list.append(pd.DataFrame(curr_data, columns=[USER_ID, UNIV_SUM, SONY_SUM, WARN_SUM, INDI_SUM, UNKN_SUM, COMB_SUM]))

        counter += 1

    track_occurrences = np.array(track_occurrences)
    print(f'Avg. number of track occurrences per user: {np.mean(track_occurrences):,}')
    print(f'Median number of track occurrences per user: {np.median(track_occurrences):,}')

    print('Completed read of Listening Events tsv')
    print('Concat single dataframes')
    df_final = pd.concat(df_list)

    print('Combine duplicate user_ids')
    df_final = df_final.groupby([USER_ID]).aggregate(np.sum).reset_index()

    df_final.to_csv(output_path, index=False)


def lfm_SI_analysis(major_count_input=OUTPUT_MAJOR_COUNTS, combine_indi_unkn=False, occ_threshold=3, length_threshold=60):
    df = pd.read_csv(major_count_input)

    if combine_indi_unkn:
        df[INDI_SUM] = df[INDI_SUM] + df[UNKN_SUM]
        df = df.drop([UNKN_SUM], axis=1)
    else:
        df[COMB_SUM] = df[COMB_SUM] - df[UNKN_SUM]
        df = df.drop([UNKN_SUM], axis=1)

    df['univ_prob'] = df[UNIV_SUM]/df[COMB_SUM]
    df['sony_prob'] = df[SONY_SUM]/df[COMB_SUM]
    df['warn_prob'] = df[WARN_SUM]/df[COMB_SUM]
    df['indi_prob'] = df[INDI_SUM]/df[COMB_SUM]
    df['SI'] = df['univ_prob'] ** 2 + df['sony_prob'] ** 2 + df['warn_prob'] ** 2 + df['indi_prob'] ** 2

    print(f'Number of users: {len(df[USER_ID]):,}')
    print(f'Number of listening-events {df[COMB_SUM].sum():,}')
    print(f'Number of users with less than {length_threshold} listening events: {len(df[df[COMB_SUM] <= length_threshold][USER_ID]):,}')
    df = df[df[COMB_SUM] > length_threshold]
    print(f'Number of filtered users: {len(df[USER_ID]):,}')
    print(f'Mean num of listening events: {(df[COMB_SUM].mean()):.2f}')
    print(f'Median num of listening events: {(df[COMB_SUM].median()):.2f}')
    plot_utils.simpson_index_distribution(df['SI'], title=f'Distribution of Simpson Indexes over unique\nlistening events per user of LFM-2b with min {occ_threshold} occ')

    major_count_collection = {}

    for threshold in SI_THRESHOLDS:
        print()
        print(f' ---------- SI >= {threshold} --------------')
        df_sub = df[df['SI'] >= threshold]

        print(f'Numbers of users: {df_sub.shape[0]}')
        if len(df_sub[USER_ID]) == 0:
            continue
        print(f'Mean playlist length: {df_sub[COMB_SUM].mean()}')
        dominant_major = 'dominant_major'
        df_sub[dominant_major] = df_sub.loc[:, [UNIV_SUM, SONY_SUM, WARN_SUM, INDI_SUM]].idxmax(axis=1)
        print(df_sub.head())

        major_count = COUNTING_DICT_EMPTY.copy()
        if combine_indi_unkn:
            del major_count[FINAL_UNKN]
        major_count[FINAL_UNIV] = df_sub[UNIV_SUM].sum()
        major_count[FINAL_SONY] = df_sub[SONY_SUM].sum()
        major_count[FINAL_WARN] = df_sub[WARN_SUM].sum()
        major_count[FINAL_INDI] = df_sub[INDI_SUM].sum()

        print(f'Num of users with dom major univ: {df_sub[df_sub[dominant_major] == UNIV_SUM].shape[0]}')
        print(f'Num of users with dom major sony: {df_sub[df_sub[dominant_major] == SONY_SUM].shape[0]}')
        print(f'Num of users with dom major warn: {df_sub[df_sub[dominant_major] == WARN_SUM].shape[0]}')
        print(f'Num of users with dom major indi: {df_sub[df_sub[dominant_major] == INDI_SUM].shape[0]}')

        print(f'Mean num of listening events for univ: {df_sub[df_sub[dominant_major] == UNIV_SUM][COMB_SUM].mean()}')
        print(f'Mean num of listening events for sony: {df_sub[df_sub[dominant_major] == SONY_SUM][COMB_SUM].mean()}')
        print(f'Mean num of listening events for warn: {df_sub[df_sub[dominant_major] == WARN_SUM][COMB_SUM].mean()}')
        print(f'Mean num of listening events for indi: {df_sub[df_sub[dominant_major] == INDI_SUM][COMB_SUM].mean()}')

        major_count_collection[threshold] = major_count

        if threshold == 1:
            df_sub.to_csv('tmp.csv', index=False)

    plot_utils.comparison_bar_plot(major_count_collection, title=f'Comparison of Simpson Index thresholds in LFM-2b\nover unique tracks per user with min {occ_threshold} occ', SI_0_name='All Users')


def main():
    #plot_utils.generate_histogram_of_record_label_distribution(sorted_albums_enriched, title='Top-level class distribution in sorted albums in LFM-2b')

    occ_threshold = 3
    lfm_enriched_analysis()
    #create_major_counts_per_user(OUTPUT_MAJOR_COUNTS, occ_threshold=occ_threshold)
    #lfm_SI_analysis(OUTPUT_MAJOR_COUNTS, occ_threshold=occ_threshold, length_threshold=30)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)


