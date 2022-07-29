import operator
import sys
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from src import constants

LABEL_MAP_TRIVIAL = constants.LABEL_MAP_TRIVIAL
LABEL_MAP_DISCOGS = constants.LABEL_MAP_DISCOGS
LABEL_MAP_WIKIPEDIA = constants.LABEL_MAP_WIKIPEDIA
LABEL_MAP_INTERIM = constants.LABEL_MAP_INTERIM
LABEL_MAP_COPYRIGHT_PREV = constants.LABEL_MAP_COPYRIGHT.replace('.csv', '_prev.csv')
LABEL_MAP_COPYRIGHT = constants.LABEL_MAP_COPYRIGHT
LABEL_MAP_FINAL = constants.LABEL_MAP_FINAL
LABEL_MAP_FINAL_STATS = constants.LABEL_MAP_FINAL_STATS

MAPS_TO_COMPARE = [LABEL_MAP_TRIVIAL, LABEL_MAP_DISCOGS, LABEL_MAP_WIKIPEDIA]

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR
CLASS_TRIVIAL = constants.CLASS_TRIVIAL
CLASS_DISCOGS = constants.CLASS_DISCOGS
CLASS_WIKIPEDIA = constants.CLASS_WIKIPEDIA
CLASS_INTERIM = constants.CLASS_INTERIM
CLASS_COPYRIGHT = constants.CLASS_COPYRIGHT

STEP_DICT = {
    CLASS_TRIVIAL: 'Trivial mapping',
    CLASS_DISCOGS: 'Discogs label crawler',
    CLASS_WIKIPEDIA: 'Wikipedia label crawler',
    CLASS_INTERIM: 'Interim mapping',
    CLASS_COPYRIGHT: 'Copyright classification',
    RECORD_LABEL_MAJOR: 'Final classification'
}

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]

EMPTY_DICT = {
    FINAL_UNIV: 0,
    FINAL_SONY: 0,
    FINAL_WARN: 0,
    FINAL_INDI: 0,
    FINAL_UNKN: 0
}

UNCLASSIFIED = 'Unclassified'

EMPTY_DICT_WITH_UNCLASSIFIED = {
    UNCLASSIFIED: 0,
    FINAL_UNIV: 0,
    FINAL_SONY: 0,
    FINAL_WARN: 0,
    FINAL_INDI: 0,
    FINAL_UNKN: 0
}


def calculate_stepwise_gain(label_map_path, selected_columns=None):
    print('Calculate stepwise gain for', label_map_path)
    df = pd.read_csv(label_map_path)
    df_chart = pd.DataFrame()

    n_tracks = df['occurrences'].sum()
    n_labels = len(df)
    if selected_columns is not None:
        classification_columns = selected_columns
    else:
        classification_columns = list(df.columns)
        classification_columns.remove(RECORD_LABEL_LOW)
        classification_columns.remove('occurrences')

    final_class_occ = EMPTY_DICT.copy()
    final_class_label = EMPTY_DICT.copy()

    print('Number of tracks in total:', n_tracks)
    print('Number of labels in total:', n_labels)

    for index, classification_column in enumerate(classification_columns):
        print()
        print('================================================================================')
        print('Stats for: ', STEP_DICT[classification_column])
        print('================================================================================')

        df_chart.loc[index, 'step'] = STEP_DICT[classification_column]
        prev_final_class_occ = final_class_occ.copy()
        prev_final_class_label = final_class_label.copy()
        final_class_occ = EMPTY_DICT.copy()
        final_class_label = EMPTY_DICT.copy()
        flags_dict_occ = {}
        flags_dict_label = {}

        df_grouped = df.groupby(classification_column)

        for classification in df_grouped.groups.keys():
            group = df_grouped.get_group(classification)
            if classification in FINALS:
                final_class_occ[classification] = group['occurrences'].sum()
                final_class_label[classification] = len(group)
            else:
                flags_dict_occ[classification] = group['occurrences'].sum()
                flags_dict_label[classification] = len(group)

        flags_dict_occ = dict(sorted(flags_dict_occ.items(), key=operator.itemgetter(1), reverse=True))

        sum_target_label = 0
        sum_target_occ = 0
        print('CLASS                          | # LABELS     (%) | # OCC           (%) | % GAIN')
        for key in EMPTY_DICT:
            label_name = key + ':'
            sum_target_label += final_class_label[key]
            sum_target_occ += final_class_occ[key]
            df_chart.loc[index, key] = final_class_occ[key]/n_tracks
            print(f'{label_name:<30} | {final_class_label[key]:<8,}({final_class_label[key]/n_labels:<6.2%}) | '
                  f'{final_class_occ[key]:<10,} ({final_class_occ[key]/n_tracks:<6.2%}) | '
                  f'+{(final_class_occ[key] - prev_final_class_occ[key])/n_tracks:.2%}')
        print('-  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  ')
        print(f'{"SUM:":<30} | {sum_target_label:<8,}({sum_target_label / n_labels:<6.2%}) | '
              f'{sum_target_occ:<10,} ({sum_target_occ / n_tracks:<6.2%}) | '
              f'+{(sum_target_occ - sum(prev_final_class_occ.values()))/n_tracks:.2%}')

        if len(flags_dict_occ) > 0:
            print('-  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  ')
            sum_flags_occ = 0
            sum_flags_label = 0
            for key in flags_dict_occ.keys():
                label_name = (key[:29] if len(key) > 29 else key) + ':'
                sum_flags_label += flags_dict_label[key]
                sum_flags_occ += flags_dict_occ[key]
                print(f'{label_name:<30} | {flags_dict_label[key]:<8,}({flags_dict_label[key]/n_labels:<6.2%}) | '
                      f'{flags_dict_occ[key]:<10,} ({flags_dict_occ[key]/n_tracks:<6.2%})')
            print('-  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  -  ')

            print(f'{"SUM flags:":<30} | {sum_flags_label:<8,}({sum_flags_label / n_labels:<6.2%}) | '
                  f'{sum_flags_occ:<10,} ({sum_flags_occ / n_tracks:<6.2%})')

            sum_total_occ = sum_target_occ + sum_flags_occ
            sum_total_label = sum_target_label + sum_flags_label
            print(f'{"SUM both:":<30} | {sum_total_label:<8,}({sum_total_label / n_labels:<6.2%}) | '
                  f'{sum_total_occ:<10,} ({sum_total_occ / n_tracks:<6.2%})')

    return df_chart


def compare_label_maps(label_map_path_1, target_class_1, label_map_path_2, target_class_2):
    label_map_1 = pd.read_csv(label_map_path_1, dtype={'wiki_url_discogs': str})
    label_map_2 = pd.read_csv(label_map_path_2, dtype={'wiki_url_discogs': str, 'wiki_keywords': str})

    if len(label_map_1) != len(label_map_2):
        print('WARNING: Label maps don\'t have same length!')
    if label_map_1['occurrences'].sum() != label_map_2['occurrences'].sum():
        print('WARNING: Label maps don\'t have same number of tracks!')

    n_tracks = label_map_1['occurrences'].sum()
    print(f'Total number of tracks: \t{n_tracks:,}')

    [final_dict_1, flags_dict_1] = create_dicts(label_map_1, target_class_1)
    [final_dict_2, flags_dict_2] = create_dicts(label_map_2, target_class_2)

    print(final_dict_1)
    print(flags_dict_1)
    print(final_dict_2)
    print(flags_dict_2)


def create_label_map_comparison(label_map_1_path, target_1, label_map_2_path, target_2):
    print('Compare finals distribution between:')
    print(label_map_1_path)
    print(label_map_2_path)
    lm_1 = pd.read_csv(label_map_1_path)
    lm_2 = pd.read_csv(label_map_2_path)
    n_diff = 0
    sum_diff = 0

    changes_map_empty = {
        FINAL_UNIV: 0,
        FINAL_SONY: 0,
        FINAL_WARN: 0,
        FINAL_INDI: 0,
        FINAL_UNKN: 0
    }

    changes_map_n = {
        FINAL_UNIV: changes_map_empty.copy(),
        FINAL_SONY: changes_map_empty.copy(),
        FINAL_WARN: changes_map_empty.copy(),
        FINAL_INDI: changes_map_empty.copy(),
        FINAL_UNKN: changes_map_empty.copy()
    }
    changes_map_occ = {
        FINAL_UNIV: changes_map_empty.copy(),
        FINAL_SONY: changes_map_empty.copy(),
        FINAL_WARN: changes_map_empty.copy(),
        FINAL_INDI: changes_map_empty.copy(),
        FINAL_UNKN: changes_map_empty.copy()
    }

    for index, entry in lm_1.iterrows():
        if lm_1.loc[index, target_1] != lm_2.loc[index, target_2]:
            major1 = lm_1.loc[index, target_1]
            major2 = lm_2.loc[index, target_2]
            occ1 = lm_1.loc[index, 'occurrences']
            occ2 = lm_2.loc[index, 'occurrences']
            changes_map_n[major1][major1] += 1
            changes_map_n[major1][major2] += 1
            changes_map_occ[major1][major1] += occ1
            changes_map_occ[major1][major2] += occ2
            n_diff += 1
            sum_diff += lm_1.loc[index, 'occurrences']

    print(f'Total number of changes: {n_diff:,}'),
    print(f'Changes on occurrence-level: {sum_diff:,}'),

    for final in FINALS:
        print(' -----------------------')
        print(final)
        print('Changes: ', changes_map_n[final][final], f' ({changes_map_occ[final][final]:,}occ)')
        for sub_final in FINALS:
            if sub_final != final and changes_map_n[final][sub_final] > 0:
                print(
                    f'{changes_map_n[final][sub_final]:,} moved to: {sub_final} (= {changes_map_occ[final][sub_final]:,}occ)')


def create_dicts(df, target_class):
    final_dict = {
        FINAL_UNIV: 0,
        FINAL_WARN: 0,
        FINAL_SONY: 0,
        FINAL_INDI: 0,
        FINAL_UNKN: 0
    }

    flags_dict = {}

    df_grouped = df.groupby(target_class)
    for classification in df_grouped.groups.keys():
        group = df_grouped.get_group(classification)
        if classification in FINALS:
            final_dict[classification] = group['occurrences'].sum()
        else:
            flags_dict[classification] = group['occurrences'].sum()

    flags_dict = dict(sorted(flags_dict.items(), key=operator.itemgetter(1), reverse=True))

    return [final_dict, flags_dict]


def create_tree_map(label_map_path, target, threshold=1000):
    df = pd.read_csv(label_map_path)
    df = df.loc[(df['occurrences'] >= threshold) & (df[target].isin(FINALS))]

    colors = px.colors.qualitative.Plotly
    fig = px.treemap(df,
                     path=[px.Constant(format_dataset_tag() + f" labels > {threshold}occ"), target, RECORD_LABEL_LOW],
                     values='occurrences',
                     color=target,
                     color_discrete_map={
                         '(?)': 'white',
                         FINAL_UNIV: colors[0],
                         FINAL_SONY: colors[1],
                         FINAL_WARN: colors[2],
                         FINAL_INDI: colors[3]
                     }
                     )
    fig.update_layout(margin=dict(t=50, l=25, r=25, b=25))
    fig.show()


def plot_stepwise_gain(df_chart):
    dataset_name = format_dataset_tag()
    df_chart = df_chart.set_index('step')
    df_chart['Unclassified'] = 1 - df_chart[list(df_chart.columns)].sum(axis=1)
    df_chart = df_chart*100
    fig = px.bar(
        df_chart,
        color_discrete_map={
            FINAL_UNIV: px.colors.qualitative.Plotly[0],
            FINAL_SONY: px.colors.qualitative.Plotly[1],
            FINAL_WARN: px.colors.qualitative.Plotly[2],
            FINAL_INDI: px.colors.qualitative.Plotly[3],
            FINAL_UNKN: 'grey',
            'Unclassified': 'white'
        },
        labels={'value': f'Percentage of tracks in {dataset_name}', 'step': 'Classification step', 'variable': 'Major Classification'},
        title=f'Record label distribution for each step for {dataset_name}'
    )
    fig.show()


def plot_stepwise_sankey(label_map_path, steps=None):
    df = pd.read_csv(label_map_path)
    df = df.replace(np.nan, UNCLASSIFIED)
    if steps is None:
        steps = list(df.columns)
        steps.remove(RECORD_LABEL_LOW)
        steps.remove('occurrences')

    nodes_dict = {
        UNCLASSIFIED + steps[0]: 0,
        FINAL_UNIV + steps[0]: 1,
        FINAL_SONY + steps[0]: 2,
        FINAL_WARN + steps[0]: 3,
        FINAL_INDI + steps[0]: 4,
        FINAL_UNKN + steps[0]: 5,
        UNCLASSIFIED: 6,
        FINAL_UNIV: 7,
        FINAL_SONY: 8,
        FINAL_WARN: 9,
        FINAL_INDI: 10,
        FINAL_UNKN: 11
    }

    changes_map_occ = {
        UNCLASSIFIED + steps[0]: EMPTY_DICT_WITH_UNCLASSIFIED.copy(),
        FINAL_UNIV + steps[0]: EMPTY_DICT_WITH_UNCLASSIFIED.copy(),
        FINAL_SONY + steps[0]: EMPTY_DICT_WITH_UNCLASSIFIED.copy(),
        FINAL_WARN + steps[0]: EMPTY_DICT_WITH_UNCLASSIFIED.copy(),
        FINAL_INDI + steps[0]: EMPTY_DICT_WITH_UNCLASSIFIED.copy(),
        FINAL_UNKN + steps[0]: EMPTY_DICT_WITH_UNCLASSIFIED.copy()
    }

    for i, entry in df.iterrows():
        major_curr = entry[steps[0]]
        major_next = entry[steps[1]]
        changes_map_occ[major_curr + steps[0]][major_next] += entry['occurrences']
        if i > 1000:
            break

    tmp = []
    for key in changes_map_occ.keys():
        for sub_key in changes_map_occ[key].keys():
            tmp.append({
                'Source': nodes_dict[key],
                'Target': nodes_dict[sub_key],
                'Value': changes_map_occ[key][sub_key],
                'Color': 'rgba(253, 227, 212, 0.5)'
            })
    df_links = pd.DataFrame.from_records(tmp)

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=list(nodes_dict.keys()),
            color="blue"
        ),
        link=dict(
            source=df_links['Source'],
            target=df_links['Target'],
            value=df_links['Value']
        ))])

    fig.update_layout(title_text="Basic Sankey Diagram", font_size=10)
    fig.show()

    return


def format_dataset_tag():
    return constants.DATASET_TAG.replace('_', '').upper()


def main(debug=None):
    global DEBUG

    if debug is not None:
        DEBUG = debug

    pd.set_option('display.width', 2000)
    pd.set_option('display.max_columns', 5)

    #create_label_map_comparison(LABEL_MAP_COPYRIGHT_PREV, CLASS_COPYRIGHT, LABEL_MAP_COPYRIGHT, CLASS_COPYRIGHT)
    create_tree_map(LABEL_MAP_FINAL, RECORD_LABEL_MAJOR)

    # df_chart = calculate_stepwise_gain(constants.LABEL_MAP_WIKIPEDIA)
    # plot_stepwise_gain(df_chart)

    plot_stepwise_sankey(LABEL_MAP_FINAL_STATS, [CLASS_INTERIM, CLASS_COPYRIGHT])
    df_chart = calculate_stepwise_gain(LABEL_MAP_FINAL_STATS)
    plot_stepwise_gain(df_chart)
    return
    print('Compare trivial + discogs')
    compare_label_maps(LABEL_MAP_TRIVIAL, CLASS_TRIVIAL, LABEL_MAP_DISCOGS, CLASS_DISCOGS)
    print('Compare discogs + wiki')
    compare_label_maps(LABEL_MAP_DISCOGS, CLASS_DISCOGS, LABEL_MAP_WIKIPEDIA, CLASS_WIKIPEDIA)
    print('Compare wiki + interim')
    compare_label_maps(LABEL_MAP_WIKIPEDIA, CLASS_WIKIPEDIA, LABEL_MAP_INTERIM, CLASS_INTERIM)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
