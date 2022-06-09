import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from wordcloud import WordCloud, STOPWORDS

from src import constants

DEBUG = constants.DEBUG

SORTED_ALBUM_URIS = constants.SORTED_ALBUM_URIS
ALBUM_URIS_WITH_LABEL_LOW = constants.ALBUM_URIS_WITH_LABEL_LOW

SORTED_LOW_LABEL_OUTPUT = constants.SORTED_LOW_LABEL_OUTPUT

BULK_FAILED_FLAG = constants.BULK_FAILED_FLAG
FAILED_LOOKUP_FLAG = constants.FAILED_LOOKUP_FLAG
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
COPYRIGHT_P = constants.COPYRIGHT_P
COPYRIGHT_C = constants.COPYRIGHT_C

UNIV_ALIAS = constants.UNIV_ALIAS
SONY_ALIAS = constants.SONY_ALIAS
WARN_ALIAS = constants.WARN_ALIAS
INDI_ALIAS = constants.INDI_ALIAS
UNKN_ALIAS = constants.UNKN_ALIAS


def base_analysis():
    low_level_label_df = pd.read_csv(ALBUM_URIS_WITH_LABEL_LOW)

    print('----------------------------------------')
    n = len(low_level_label_df)
    label_assigned = low_level_label_df[RECORD_LABEL_LOW].count()
    copyright_p = low_level_label_df[COPYRIGHT_P].count()
    copyright_c = low_level_label_df[COPYRIGHT_C].count()
    bulk_errors = len(low_level_label_df.loc[low_level_label_df[RECORD_LABEL_LOW] == BULK_FAILED_FLAG])
    lookup_errors = len(low_level_label_df.loc[low_level_label_df[RECORD_LABEL_LOW] == FAILED_LOOKUP_FLAG])
    print(f'Total number of URIs: \t\t{n:,}')
    print(f'Assigned labels: \t\t\t{label_assigned:,} ({(label_assigned/n):.2%} of total URIs)')
    print(f'Assigned copyright-P: \t\t{copyright_p:,} ({(copyright_p/n):.2%} of total URIs)')
    print(f'Assigned copyright-C: \t\t{copyright_c:,} ({(copyright_c/n):.2%} of total URIs)')
    print(f'Bulk errors: \t\t\t\t{bulk_errors:,} ({(bulk_errors/n):.2%} of total URIs)')
    print(f'Failed lookups: \t\t\t{lookup_errors:,} ({(lookup_errors/n):.2%} of total URIs)')

    print('----------------------------------------')
    low_level_label_df_clean = low_level_label_df.loc[(low_level_label_df[RECORD_LABEL_LOW] != BULK_FAILED_FLAG) & (low_level_label_df[RECORD_LABEL_LOW] != FAILED_LOOKUP_FLAG)]

    n_tracks = low_level_label_df['occurrences'].sum()
    label_grouped_track_df = low_level_label_df.groupby([RECORD_LABEL_LOW])['occurrences'].sum().reset_index(name='track_occurrences').sort_values(['track_occurrences'], ascending=False).reset_index(drop=True)
    label_coverage_on_tracks = label_grouped_track_df['track_occurrences'].sum()
    print(f'Total number of tracks:\t\t{n_tracks:,}')
    print(f'Coverage on track level:\t{(label_coverage_on_tracks/n_tracks):.2%}')

    print('----------------------------------------')
    label_grouped_df = low_level_label_df.groupby([RECORD_LABEL_LOW])['album_uri'].count().reset_index(
        name='label_occurrences').sort_values(['label_occurrences'], ascending=False).reset_index(drop=True)
    n_labels = len(label_grouped_df)
    assigned_labels_clear = label_grouped_df['label_occurrences'].sum()
    print(
        f'Number of unique labels: \t{n_labels:,} ({(n_labels / assigned_labels_clear):.2%} of total assigned labels)')

    matching_univ = label_grouped_df.loc[label_grouped_df[RECORD_LABEL_LOW].str.contains(UNIV_ALIAS, case=False)]
    n_univ = matching_univ['label_occurrences'].count()
    coverage_univ = matching_univ['label_occurrences'].sum()

    matching_warn = label_grouped_df.loc[label_grouped_df[RECORD_LABEL_LOW].str.contains(WARN_ALIAS, case=False)]
    n_warn = matching_warn['label_occurrences'].count()
    coverage_warn = matching_warn['label_occurrences'].sum()

    matching_sony = label_grouped_df.loc[label_grouped_df[RECORD_LABEL_LOW].str.contains(SONY_ALIAS, case=False)]
    n_sony = matching_sony['label_occurrences'].count()
    coverage_sony = matching_sony['label_occurrences'].sum()

    matching_indi = label_grouped_df.loc[label_grouped_df[RECORD_LABEL_LOW].str.contains(INDI_ALIAS, case=False)]
    n_indi = matching_indi['label_occurrences'].count()
    coverage_indi = matching_indi['label_occurrences'].sum()

    # for independent a full match is used, as there are weaker alias (e.g. 'None')
    matching_unkn = label_grouped_df.loc[label_grouped_df[RECORD_LABEL_LOW].str.fullmatch(UNKN_ALIAS, case=False)]
    n_unkn = matching_unkn['label_occurrences'].count()
    coverage_unkn = matching_unkn['label_occurrences'].sum()

    print(f'Containing \'Universal\': \t{n_univ:,} ({(coverage_univ / n_labels):.2%} of label coverage)')
    print(f'Containing \'Warner|WM\': \t{n_warn:,} ({(coverage_warn / n_labels):.2%} of label coverage)')
    print(f'Containing \'Sony\': \t\t\t{n_sony:,} ({(coverage_sony / n_labels):.2%} of label coverage)')
    print(f'Containing \'Independent\': \t{n_indi:,} ({(coverage_indi / n_labels):.2%} of label coverage)')
    print(f'Containing \'Unknown\': \t\t{n_unkn:,} ({(coverage_unkn / n_labels):.2%} of label coverage)')
    sum_target = n_univ + n_warn + n_sony + n_indi + n_unkn
    sum_coverage = coverage_univ + coverage_warn + coverage_sony + coverage_indi + coverage_unkn
    print(f'Sum target labels: \t\t\t{sum_target:,} ({(sum_coverage / n_labels):.2%} of label coverage)')
    missing_target = n_labels - sum_target
    missing_coverage = n_labels - sum_coverage
    print(f'Missing labels: \t\t\t{missing_target:,} ({(missing_coverage / n_labels):.2%} of label coverage)')

    print('----------------------------------------')
    print('Plotting graphs')


    #n, bins, patches = plt.hist(label_grouped_df['label_occurrences'], bins=50)
    #plt.yscale('log')
    #plt.xlabel('Record Labels')
    #plt.ylabel('Occurrences')

    threshold = 0.9
    curr_sum = 0
    total_sum = label_grouped_track_df['track_occurrences'].sum()
    index = 0
    for index, entry in tqdm(label_grouped_track_df.iterrows(), total=len(label_grouped_track_df)):
        curr_sum += entry['track_occurrences']
        if curr_sum/total_sum >= threshold:
            break
    print(
        f'First {index:,} low-level labels cover {threshold:.0%} of all occurrences (= {index / len(label_grouped_track_df):.2%})')

    fig1, ax1 = plt.subplots()
    ax1.set_title('Distribution of sorted low-level record labels')
    ax1.set_yscale('log')
    ax1.set_xlabel('Low-level record labels')
    ax1.set_ylabel('Occurrences (log)')
    plt.axvline(x=index, color='red', linewidth=0.5, label=f'Top {(index / len(label_grouped_track_df)):.2%} of all unique low-level record labels cover {threshold:.0%}\n of all occurrences ({index:,} of {len(label_grouped_track_df):,})')
    plt.legend()
    plt.plot(label_grouped_track_df['track_occurrences'])

    # TODO: Add plot of low-level label distribution on label leven (instead of track level)?
    # label_grouped_track_df.reset_index(inplace=True, drop=True)
    # ax1.set_title('Distribution of low level record labels')
    # ax1.set_yscale('log')
    # ax1.set_xlabel('Low level record labels')
    # ax1.set_ylabel('Occurrences')
    # plt.axvline(x=index, color='red', linewidth=0.5, label=f'Sum of occurrences above {threshold:.0%} at {index / len(label_grouped_track_df):.2%}')
    # plt.legend()
    # plt.plot(label_grouped_df['label_occurrences'])

    threshold = 1000
    # TODO: fix treemap
    tmp_df = label_grouped_track_df.loc[label_grouped_track_df['track_occurrences'] >= threshold]
    # fig = px.treemap(tmp_df, path=[px.Constant(f"MPD low-level record labels with occ > {threshold}"), RECORD_LABEL_LOW], values='track_occurrences',
    #                  color='track_occurrences',
    #                  color_continuous_scale='RdBu',
    #                  color_continuous_midpoint=np.average(tmp_df['track_occurrences'], weights=tmp_df['track_occurrences']))
    # fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    # fig.show()

    fig3, ax3 = plt.subplots(figsize=(20, 10))
    label_names = " ".join(str(label_name) for label_name in low_level_label_df_clean[RECORD_LABEL_LOW])
    stopwords = set(STOPWORDS)
    stopwords.update(
        ['Music', 'Record', 'Entertainment', 'Records', 'Recording', 'Production', 'Media', 'LLC.', 'Music Group',
         'Sound', 'Inc', 'LLC', 'Co', 'Recordings', 'Ent', 'Group', 'Productions', 'Ltd', 'Company', 'Publishing',
         'Label'])
    max_words = 100
    wordcloud = WordCloud(
        width=800,
        height=400,
        stopwords=stopwords,
        max_words=max_words,
        background_color='white'
    ).generate(label_names)
    plt.imshow(wordcloud, interpolation='bilinear')
    # ax3.set_title(f'Word cloud of top {max_words} low level record labels:')
    ax3.axis('off')

    #fig4, ax4 = plt.subplots()
    #labels = ['Universal', 'Sony', 'Warner', 'Independent', 'Unknown', 'Missing']
    #shares = [n_univ, n_sony, n_warn, n_indi, n_unkn, missing_target]
    #ax4.set_title('Assigned labels total: ')
    #ax4.pie(shares, labels=labels, autopct='%1.1f%%', startangle=90)

    fig5, ax5 = plt.subplots()
    labels = ['Universal', 'Sony', 'Warner', 'Independent', 'Unknown', 'Missing']
    shares = [coverage_univ, coverage_sony, coverage_warn, coverage_indi, coverage_unkn, missing_coverage]
    ax5.set_title('Assigned labels relative to label occ: ')
    ax5.pie(shares, labels=labels, autopct='%1.1f%%', startangle=90)

    #sns.displot(data=label_grouped_df['label_occurrences'], log_scale=True, kind='hist')
    plt.show()


def main(debug=None):
    global DEBUG

    if debug is not None:
        DEBUG = debug

    pd.set_option('display.width', 1000)
    pd.set_option('display.max_columns', 5)
    base_analysis()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)