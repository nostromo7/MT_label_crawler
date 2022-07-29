import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.colors as mc
import colorsys
import numpy as np
import pandas as pd
from tqdm import tqdm

from src import constants

OCC = constants.OCC
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]

short_name_dict = {
    constants.FINAL_UNIV: 'Universal',
    constants.FINAL_SONY: 'Sony',
    constants.FINAL_WARN: 'Warner',
    constants.FINAL_INDI: 'Independent',
    constants.FINAL_UNKN: 'Unknown'
}

def pie_plot(input_dict, title, colors=None, show_legend=True):

    input_dict = {key: value for key, value in input_dict.items() if value > 0}

    fig, ax = plt.subplots()
    labels = input_dict.keys()
    shares = input_dict.values()
    ax.set_title(title)
    ax.pie(shares, colors=colors, autopct='%1.1f%%', startangle=90)
    if show_legend:
        ax.legend(labels, loc='lower left')
    plt.show()


def bar_plot(input_dict, title, colors=None, show_legend=True):

    input_dict = {short_name_dict[key]: value for key, value in input_dict.items() if value > 0}

    df = pd.DataFrame.from_dict(input_dict, orient='index', columns=['value'])
    sum_values = np.sum(df['value'])
    df['share'] = (df['value']/sum_values)*100
    print(df)

    fig, ax = plt.subplots()
    ax.set_title(title)
    df['share'].plot.bar(color=colors, ax=ax)
    plt.xticks(rotation=0)

    i = 0
    for p in ax.patches:
        x, y = p.get_xy()
        plt.text(x=x+p.get_width()/2,
                 y=y+p.get_height()/2,
                 s=str(round(df['share'][i], 2)) + '%',
                 ha='center',
                 rotation=0)
        i += 1
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    ax.set_ylim([0, 55])
    plt.show()


def comparison_bar_plot(input_dict_collection, title, show_percentages=False, SI_0_name='All Playlists', SI_1_name='SI = 1'):
    df_list = []
    thresholds = list(input_dict_collection.keys())
    threshold_names = [(r'$SI \geq $' + str(t) if float(t) > 0 else SI_0_name) for t in thresholds]
    share_names = [('share_' + str(t)) for t in thresholds]
    for threshold in thresholds:
        df_list.append(pd.DataFrame.from_dict(input_dict_collection[threshold], orient='index', columns=[threshold]))
    df = pd.concat(df_list, axis=1)
    df = df.rename(index=short_name_dict)

    for threshold in thresholds:
        df['share_' + str(threshold)] = (df[threshold]/df[threshold].sum()) * 100

    fig, ax = plt.subplots()
    ax.set_title(title)

    colors = []
    base_color = 'tab:blue'
    count = len(input_dict_collection.keys())
    increase = 0.9/(count - 1)
    for key in input_dict_collection.keys():
        alpha = 0.1 + (increase * count)
        c = colorsys.rgb_to_hls(*mc.to_rgb(base_color))
        colors.append(colorsys.hls_to_rgb(c[0], alpha * (1-c[1]), c[2]))
        count -= 1

    df.plot.bar(y=share_names, use_index=True, ax=ax, color=colors)

    plt.xticks(rotation=0)

    if show_percentages:
        i = 0
        threshold_counter = 0
        num_majors = df.shape[0]

        threshold = thresholds[threshold_counter]
        for p in ax.patches:
            if i == num_majors:
                i = 0
                threshold_counter += 1
                threshold = thresholds[threshold_counter]

            x, y = p.get_xy()
            plt.text(x=(x + p.get_width() / 2),
                     y=(y + p.get_height() / 2),
                     s=str(round(df['share_' + str(threshold)][i] * 100, 2)) + ' %',
                     ha='center',
                     rotation=90)
            i += 1

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    plt.legend(threshold_names)
    plt.show()


def simpson_index_distribution(simpson_index_list, title, num_buckets=30):
    bins = np.linspace(0.2, 1, num_buckets)
    (n2, bins2, patches) = plt.hist(simpson_index_list, bins, rwidth=0.85)
    plt.yscale('log')
    plt.title(title)
    plt.xlabel("Simpson Index")
    plt.ylabel("Occurrences of Simpson Index")
    max = n2.max()
    max_index = bins2[np.where(n2 == max)]

    ones = n2[-1]
    ones_index = bins[-2]

    plt.annotate(str(int(max)), xy=(max_index, max + (0.05 * max)), color='red')
    plt.annotate(str(int(ones)), xy=(ones_index, ones + (0.05 * ones)), color='red')
    plt.show()


def generate_histogram_of_record_label_distribution(sorted_albums_enriched_path, title="Label distribution per histogram bucket", number_of_buckets=20):
    sorted_albums_enriched_df = pd.read_csv(sorted_albums_enriched_path)

    number_of_albums = sorted_albums_enriched_df.shape[0]
    number_of_tracks = sorted_albums_enriched_df[OCC].sum()
    size_of_bucket = round(number_of_tracks / number_of_buckets)

    curr_track_count = 0
    known = 0

    sony_counter = 0
    univ_counter = 0
    warn_counter = 0
    indi_counter = 0

    sony_collection = []
    univ_collection = []
    warn_collection = []
    indi_collection = []

    album_counter = 0

    for i, entry in tqdm(sorted_albums_enriched_df.iterrows(), total=sorted_albums_enriched_df.shape[0]):
        curr_occ = entry[OCC]
        curr_track_count += curr_occ
        album_counter += 1

        major = entry[RECORD_LABEL_MAJOR]
        if major in FINALS:
            known += curr_occ
            if major == FINAL_SONY:
                sony_counter += curr_occ
            elif major == FINAL_UNIV:
                univ_counter += curr_occ
            elif major == FINAL_WARN:
                warn_counter += curr_occ
            elif major == FINAL_INDI:
                indi_counter += curr_occ

        if curr_track_count >= size_of_bucket:
            sony_collection.append(sony_counter)
            univ_collection.append(univ_counter)
            warn_collection.append(warn_counter)
            indi_collection.append(indi_counter)

            print("bucket has {} albums ({}%)".format(album_counter, round(100*(album_counter/ number_of_albums),4)))
            album_counter = 0

            known = 0
            curr_track_count = 0
            sony_counter = 0
            univ_counter = 0
            warn_counter = 0
            indi_counter = 0

    sony_collection.append(sony_counter)
    univ_collection.append(univ_counter)
    warn_collection.append(warn_counter)
    indi_collection.append(indi_counter)
    print("bucket has {} albums ({}%)".format(album_counter, round(100 * (album_counter / number_of_albums), 4)))

    r = range(1, number_of_buckets+1)
    raw_data = {'sony': sony_collection,
                'univ': univ_collection,
                'warn': warn_collection,
                'indi': indi_collection}
    df = pd.DataFrame(raw_data)

    totals = [i + j + k + l for i, j, k, l in zip(df['sony'],
                                                  df['univ'],
                                                  df['warn'],
                                                  df['indi'])]

    sony_bars = [i / j * 100 for i, j in zip(df['sony'], totals)]
    univ_bars = [i / j * 100 for i, j in zip(df['univ'], totals)]
    warn_bars = [i / j * 100 for i, j in zip(df['warn'], totals)]
    indi_bars = [i / j * 100 for i, j in zip(df['indi'], totals)]

    plt.bar(r, univ_bars, edgecolor='white', label='Universal', color='lightblue')
    plt.bar(r, sony_bars, bottom=univ_bars, edgecolor='white', label='Sony', color='tomato')
    plt.bar(r, warn_bars, bottom=[i + j for i, j in zip(univ_bars, sony_bars)], edgecolor='white', label='Warner', color='lightgreen')
    plt.bar(r, indi_bars, bottom=[i + j + k for i, j, k in zip(univ_bars, sony_bars, warn_bars)], edgecolor='white', label='Independent', color='lavender')

    plt.title(title)
    plt.xlabel('Sorted buckets')
    plt.ylabel('Percentage of label')
    plt.xlim(left=0)
    plt.xticks([round(elem, 1) for elem in list(r)])
    plt.legend(loc='best')
    plt.show()

