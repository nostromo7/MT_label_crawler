"""
    This script was taken from the Million Playlist Dataset from AIcrowd and was adapted slightly to show
    additional stats, including:
    - median of number of tracks per playlist
    - plots of track, album and artist distribution

    iterates over the million playlist dataset and outputs info
    about what is in there.

    Usage:

        python stats.py path-to-mpd-data
"""
import statistics
import json
import re
import collections
import os
import datetime
from tqdm import tqdm
import matplotlib.pyplot as plt


total_playlists = 0
total_tracks = 0
tracks = set()
artists = set()
albums = set()
titles = set()
total_descriptions = 0
ntitles = set()
n_tracks = list()
title_histogram = collections.Counter()
artist_histogram = collections.Counter()
album_histogram = collections.Counter()
track_histogram = collections.Counter()
last_modified_histogram = collections.Counter()
num_edits_histogram = collections.Counter()
playlist_length_histogram = collections.Counter()
num_followers_histogram = collections.Counter()

quick = False
max_files_for_quick_processing = 5


def process_mpd(path):
    count = 0
    filenames = sorted(os.listdir(path))
    for filename in tqdm(filenames, total=len(filenames)):
        if filename.startswith("mpd.slice.") and filename.endswith(".json"):
            fullpath = os.sep.join((path, filename))
            f = open(fullpath)
            js = f.read()
            f.close()
            mpd_slice = json.loads(js)
            process_info(mpd_slice["info"])
            for playlist in mpd_slice["playlists"]:
                process_playlist(playlist)
            count += 1

            if quick and count > max_files_for_quick_processing:
                break

    show_summary()


def show_summary():
    print()
    print("number of playlists", total_playlists)
    print("number of tracks", total_tracks)
    print("number of unique tracks", len(tracks))
    print("number of unique albums", len(albums))
    print("number of unique artists", len(artists))
    print("number of unique titles", len(titles))
    print("number of playlists with descriptions", total_descriptions)
    print("number of unique normalized titles", len(ntitles))
    print("avg playlist length", float(total_tracks) / total_playlists)
    print("med playlist length", statistics.median(n_tracks))
    print()
    print("top playlist titles")
    for title, count in title_histogram.most_common(20):
        print("%7d %s" % (count, title))

    print()
    print("top tracks")
    for track, count in track_histogram.most_common(20):
        print("%7d %s" % (count, track))

    print()
    print("top artists")
    for artist, count in artist_histogram.most_common(20):
        print("%7d %s" % (count, artist))

    print()
    print("numedits histogram")
    for num_edits, count in num_edits_histogram.most_common(20):
        print("%7d %d" % (count, num_edits))

    print()
    print("last modified histogram")
    for ts, count in last_modified_histogram.most_common(20):
        print("%7d %s" % (count, to_date(ts)))

    print()
    print("playlist length histogram")
    for length, count in playlist_length_histogram.most_common(20):
        print("%7d %d" % (count, length))

    print()
    print("num followers histogram")
    for followers, count in num_followers_histogram.most_common(20):
        print("%7d %d" % (count, followers))

    analyze_histogram(track_histogram, plot_title='Distribution of sorted track occurrences in MPD', plot_focus='tracks')
    analyze_histogram(album_histogram, plot_title='Distribution of sorted album occurrences in MPD', plot_focus='albums')
    analyze_histogram(artist_histogram, plot_title='Distribution of sorted artist occurrences in MPD', plot_focus='artists')


def normalize_name(name):
    name = name.lower()
    name = re.sub(r"[.,\/#!$%\^\*;:{}=\_`~()@]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def to_date(epoch):
    return datetime.datetime.fromtimestamp(epoch).strftime("%Y-%m-%d")


def process_playlist(playlist):
    global total_playlists, total_tracks, total_descriptions

    total_playlists += 1
    n_tracks.append(playlist["num_tracks"])
    # print playlist['playlist_id'], playlist['name']

    if "description" in playlist:
        total_descriptions += 1

    titles.add(playlist["name"])
    nname = normalize_name(playlist["name"])
    ntitles.add(nname)
    title_histogram[nname] += 1

    playlist_length_histogram[playlist["num_tracks"]] += 1
    last_modified_histogram[playlist["modified_at"]] += 1
    num_edits_histogram[playlist["num_edits"]] += 1
    num_followers_histogram[playlist["num_followers"]] += 1

    for track in playlist["tracks"]:
        total_tracks += 1
        albums.add(track["album_uri"])
        tracks.add(track["track_uri"])
        artists.add(track["artist_uri"])

        full_name = track["track_name"] + " by " + track["artist_name"]
        artist_histogram[track["artist_name"]] += 1
        album_histogram[track["album_name"]] += 1
        track_histogram[full_name] += 1


def analyze_histogram(histogram, perc=0.9, plot_title=None, plot_focus=None):
    sorted_hist = sorted(histogram.values(), reverse=True)

    overall_sum = sum(sorted_hist)
    number_total = len(sorted_hist)

    current_sum = 0
    index = 0
    for index, value in enumerate(sorted_hist):
        current_sum += value
        if current_sum/overall_sum >= perc:
            print(f'above {perc:.2%} at: {index:,} ({(index / number_total):.2%})')
            break

    plt.plot(sorted_hist)
    plt.title(plot_title)
    plt.xlabel('Unique ' + plot_focus)
    plt.axvline(x=index, color='red', linewidth=0.5, label=f'Top {(index / number_total):.2%} of all unique {plot_focus} cover {perc:.0%}\n of all occurrences ({index:,} of {number_total:,})')
    plt.ylabel('Occurrences in MPD (log)')
    plt.yscale('log')
    plt.legend()
    plt.show()


def process_info(_):
    pass


if __name__ == "__main__":
    quick = False
    process_mpd('../../data/slices_original')
