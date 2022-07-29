import sys
import os
import pandas as pd
import json
from tqdm import tqdm
from src import constants
from src.utils import plot_utils

ALBUM_URIS_ENRICHED = '../../data/generated/sorted_album_uris_enriched_mpd.csv'
CHALLENGE_PATH = '../../data/challenge_set.json'
TRACK_TO_ALBUM_MAP = '../../data/experiment_output/mpd_track_to_album_map.csv'
SUBMISSION_PATH = '../../data/experiment_output/submission_KAENEN_500.csv'
SUBMISSION_COLLECTION_PATH = '../../data/experiment_output/experiment_output_collection.csv'

PATH_TO_SLICES = constants.PATH_TO_SLICES_ORIGINAL

ALBUM_URI = constants.ALBUM_URI
TRACK_URI = constants.TRACK_URI
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI

EMPTY_DICT = {
    FINAL_UNIV: 0,
    FINAL_SONY: 0,
    FINAL_WARN: 0,
    FINAL_INDI: 0
}


def create_track_to_album_mapping():
    track_set = set()
    track_to_album_map = {}

    all_slices = os.listdir(PATH_TO_SLICES)
    for filename in tqdm(all_slices, total=len(all_slices)):
        if filename.startswith('mpd.slice') and filename.endswith('.json'):
            with open(PATH_TO_SLICES + filename, 'r') as read_file:
                for playlist in json.load(read_file)['playlists']:
                    for track in playlist['tracks']:
                        if track[TRACK_URI] not in track_set:
                            track_set.add(track[TRACK_URI])
                            track_to_album_map[track[TRACK_URI]] = track[ALBUM_URI]

    track_map_df = pd.DataFrame.from_dict(track_to_album_map, orient='index', columns=[ALBUM_URI])
    print(track_map_df)
    track_map_df.to_csv(TRACK_TO_ALBUM_MAP)


def create_SI_comparison(len_threshold=5, SI_thresholds=[0,0.6,1]):
    df_album_map = pd.read_csv(ALBUM_URIS_ENRICHED)
    mapping_dict = dict(zip(df_album_map[ALBUM_URI], df_album_map[RECORD_LABEL_MAJOR]))
    playlist_counter = 0
    df_list = []

    with open(CHALLENGE_PATH, 'r') as read_file:
        challenge_playlists = json.load(read_file)['playlists']
        playlist_list = []
        for playlist in challenge_playlists:
            if len(playlist['tracks']) >= len_threshold:
                playlist_counter += 1
                counting_dict = EMPTY_DICT.copy()

                for track in playlist['tracks']:
                    counting_dict[mapping_dict[track['album_uri']]] += 1

                playlist_list.append([
                    playlist['pid'],
                    counting_dict[FINAL_UNIV],
                    counting_dict[FINAL_SONY],
                    counting_dict[FINAL_WARN],
                    counting_dict[FINAL_INDI],
                    len(playlist['tracks'])
                ])
        df_list.append(pd.DataFrame(playlist_list, columns=['pid', 'univ_sum', 'sony_sum', 'warn_sum', 'indi_sum', 'comb_sum']))

    df_final = pd.concat(df_list)

    df_final['univ_prob'] = df_final['univ_sum'] / df_final['comb_sum']
    df_final['sony_prob'] = df_final['sony_sum'] / df_final['comb_sum']
    df_final['warn_prob'] = df_final['warn_sum'] / df_final['comb_sum']
    df_final['indi_prob'] = df_final['indi_sum'] / df_final['comb_sum']
    df_final['SI'] = df_final['univ_prob'] ** 2 + df_final['sony_prob'] ** 2 + df_final['warn_prob'] ** 2 + df_final[
        'indi_prob'] ** 2

    major_count_collection = {}

    for threshold in SI_thresholds:
        df_sub = df_final[df_final['SI'] >= threshold]
        print()
        print(f' ---------- SI >= {threshold} --------------')
        print(f'Distribution of playlists with simpson index >= {threshold}')
        print(f"Number of playlists with SI >= {threshold}: {df_sub['pid'].size:,}")
        print(f"Number of tracks with SI >= {threshold}: {df_sub['comb_sum'].sum():,}")

        major_count = EMPTY_DICT.copy()
        major_count[FINAL_UNIV] = df_sub['univ_sum'].sum()
        major_count[FINAL_SONY] = df_sub['sony_sum'].sum()
        major_count[FINAL_WARN] = df_sub['warn_sum'].sum()
        major_count[FINAL_INDI] = df_sub['indi_sum'].sum()
        major_count_collection[threshold] = major_count

        dominant_major = 'dominant_major'
        df_sub[dominant_major] = df_sub.loc[:, ['univ_sum', 'sony_sum', 'warn_sum', 'indi_sum']].idxmax(axis=1)
        print(f"Avg. number of tracks per playlist where univ is dominant: {df_sub[df_sub[dominant_major] == 'univ_sum']['comb_sum'].mean()}")
        print(f"Avg. number of tracks per playlist where sony is dominant: {df_sub[df_sub[dominant_major] == 'sony_sum']['comb_sum'].mean()}")
        print(f"Avg. number of tracks per playlist where warn is dominant: {df_sub[df_sub[dominant_major] == 'warn_sum']['comb_sum'].mean()}")
        print(f"Avg. number of tracks per playlist where indi is dominant: {df_sub[df_sub[dominant_major] == 'indi_sum']['comb_sum'].mean()}")
        print(f"Avg. number of tracks per playlist {df_sub['comb_sum'].mean()}")

        major_count_sum = sum(major_count.values())
        print(f'Share of univ: {(major_count[FINAL_UNIV] / major_count_sum):.2%}')
        print(f'Share of sony: {(major_count[FINAL_SONY] / major_count_sum):.2%}')
        print(f'Share of warn: {(major_count[FINAL_WARN] / major_count_sum):.2%}')
        print(f'Share of indi: {(major_count[FINAL_INDI] / major_count_sum):.2%}')

    plot_utils.comparison_bar_plot(major_count_collection, title=f"Comparison of Simpson Index thresholds\nfor playlists with min length {len_threshold} in RecSys18 challenge set")


def explore_challenge_set(len_threshold=5, exclude_ties=True, SI_threshold=None, print_analysis=False):
    df_album_map = pd.read_csv(ALBUM_URIS_ENRICHED)
    mapping_dict = dict(zip(df_album_map[ALBUM_URI], df_album_map[RECORD_LABEL_MAJOR]))
    dominant_major_per_playlist_dict = {}
    dominant_major_per_playlist_count = EMPTY_DICT.copy()
    playlist_counter = 0

    SI_collection = []

    with open(CHALLENGE_PATH, 'r') as read_file:
        challenge_playlists = json.load(read_file)['playlists']

        for playlist in challenge_playlists:
            if len(playlist['tracks']) >= len_threshold:
                playlist_counter += 1
                counting_dict = EMPTY_DICT.copy()

                for track in playlist['tracks']:
                    counting_dict[mapping_dict[track['album_uri']]] += 1

                dom_major = max(counting_dict, key=counting_dict.get)

                if exclude_ties:
                    max_count = 0
                    for count in list(counting_dict.values()):
                        if count == counting_dict[dom_major]:
                            max_count += 1
                    if max_count > 1:
                        continue

                if SI_threshold is not None:
                    playlist_length = sum(counting_dict.values())
                    SI = (counting_dict[FINAL_UNIV]/playlist_length) ** 2 + (counting_dict[FINAL_SONY]/playlist_length) ** 2 + (counting_dict[FINAL_WARN]/playlist_length) ** 2 + (counting_dict[FINAL_INDI]/playlist_length) ** 2
                    SI_collection.append(SI)
                    if SI < SI_threshold:
                        continue

                dominant_major_per_playlist_dict[playlist['pid']] = dom_major
                dominant_major_per_playlist_count[dom_major] += 1

    if print_analysis:
        plot_utils.simpson_index_distribution(SI_collection, title=f"Distribution of Simpson Indexes for playlists in challenge set\nwith min length {len_threshold} ({playlist_counter:,} playlists)")

    print(dominant_major_per_playlist_count)
    print(len(dominant_major_per_playlist_dict))
    return dominant_major_per_playlist_dict


def rerank_submission(dominant_major_per_playlist_dict, submission_length=500, rerank_limit=10, description='reranked'):
    df_album_map = pd.read_csv(ALBUM_URIS_ENRICHED)
    album_to_major_map = dict(zip(df_album_map[ALBUM_URI], df_album_map[RECORD_LABEL_MAJOR]))

    df_track_to_album_map = pd.read_csv(TRACK_TO_ALBUM_MAP, names=[TRACK_URI, ALBUM_URI], header=None, skiprows=1)
    track_to_album_map = dict(zip(df_track_to_album_map[TRACK_URI], df_track_to_album_map[ALBUM_URI]))
    submission = pd.read_csv(SUBMISSION_PATH, skiprows=3, header=None)
    reranked_submission_dict = {}
    for index, entry in tqdm(submission.iterrows(), total=submission.shape[0]):
        pid = entry[0]
        tracks = entry[1:len(entry)].values

        if pid in dominant_major_per_playlist_dict.keys():
            dom_major = dominant_major_per_playlist_dict[pid]
            major_tracks = []
            rest_tracks = list(tracks.copy())
            counter = 0
            for track in tracks:
                album_uri = track_to_album_map[track]
                major = album_to_major_map[album_uri]
                if major == dom_major:
                    major_tracks.append(track)
                    rest_tracks.remove(track)

                counter += 1
                if counter == rerank_limit:
                    break
            tracks = major_tracks + rest_tracks
        tracks = tracks[0:submission_length]

        reranked_submission_dict[pid] = tracks

    print(submission.shape)
    print(len(reranked_submission_dict))

    fh = open(SUBMISSION_PATH.replace('.csv', '_' + description + '.csv'), 'w+')

    fh.write("#SUBMISSION")
    fh.write('\n')
    fh.write('team_info,{},{}'.format('KAENEN_' + description, 'e01426077@student.tuwien.ac.at'))
    fh.write('\n')

    for pid in reranked_submission_dict.keys():
        fh.write('\n')
        fh.write(str(pid))

        fh.write(',' + ','.join(map(str, reranked_submission_dict[pid])))

    fh.write('\n')
    fh.close()


def main():
    pd.set_option("display.precision", 16)
    pd.set_option('display.max_columns', 4)
    pd.set_option('display.width', 1000)

    submission_collection = pd.read_csv(SUBMISSION_COLLECTION_PATH, index_col='id')
    submission_collection = submission_collection.sort_values(by=['NDCG'], ascending=False)
    baseline_NDCG = submission_collection.loc[192948]['NDCG']
    submission_collection['gain'] = ((submission_collection['NDCG'] / baseline_NDCG) - 1) * 100

    submission_collection = submission_collection.round({
        'min_length': 0,
        'rerank_first_n_tracks': 0,
        'SI_threshold': 1,
        'Playlists_changed': 0,
        'R-Prec': 6,
        'clicks': 4,
        'NDCG': 7,
        'gain': 6
    })

    print(submission_collection[['note', 'R-Prec', 'clicks', 'NDCG', 'gain']])

    submission_collection[['min_length','rerank_first_n_tracks','SI_threshold','Playlists_changed','R-Prec','clicks','NDCG','gain']].to_csv('tmp.csv', sep='&', index=False)

    min_length = 25
    rerank_first_n_tracks = 10
    SI_threshold = None
    description = f"reranked_min{min_length}_first{rerank_first_n_tracks}_si{str(SI_threshold).replace('.','')}"
    print('current: ' + description)
    create_track_to_album_mapping()
    create_SI_comparison(len_threshold=min_length, SI_thresholds=[0, 0.7, 0.8, 0.9, 1])
    dominant_major_per_playlist_dict = explore_challenge_set(len_threshold=min_length, SI_threshold=SI_threshold, print_analysis=False)
    rerank_submission(dominant_major_per_playlist_dict, rerank_limit=rerank_first_n_tracks, description=description)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
