import pandas as pd
from tqdm import tqdm

from src import constants

###################
# This script is a handy tool to decouple the dependence of the single classification steps (e.g. running the discogs
# crawler before the wikipedia crawler) by fast forwarding all made classifications to the successive steps.
###################

LABEL_MAP_TRIVIAL = constants.LABEL_MAP_TRIVIAL
LABEL_MAP_DISCOGS = constants.LABEL_MAP_DISCOGS
LABEL_MAP_DISCOGS_EXT = constants.LABEL_MAP_DISCOGS_EXT
LABEL_MAP_WIKIPEDIA = constants.LABEL_MAP_WIKIPEDIA
LABEL_MAP_WIKIPEDIA_EXT = constants.LABEL_MAP_WIKIPEDIA_EXT
LABEL_MAP_INTERIM = constants.LABEL_MAP_INTERIM

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

FINALS = [FINAL_UNIV, FINAL_SONY, FINAL_WARN, FINAL_INDI, FINAL_UNKN]

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW

CLASS_TRIVIAL = constants.CLASS_TRIVIAL
CLASS_DISCOGS = constants.CLASS_DISCOGS
CLASS_WIKIPEDIA = constants.CLASS_WIKIPEDIA
CLASS_INTERIM = constants.CLASS_INTERIM

'''
Maps all entries from df1 to df2 where target1 is in the FINALS
'''
def forward_mapping(df1, target1, df2, target2):
    all_columns = list(df2.columns)
    all_columns.remove('occurrences')
    all_columns.remove(RECORD_LABEL_LOW)
    all_columns.remove(target2)
    selected_columns = []
    for column in all_columns:
        if column in set(df1.columns):
            selected_columns.append(column)

    for index, entry in tqdm(df1.iterrows(), total=df1.shape[0]):
        if entry[target1] in FINALS:
            df2.loc[index, selected_columns] = entry[selected_columns]
            df2.loc[index, target2] = entry[target1]
    return df2

label_map_triv = pd.read_csv(LABEL_MAP_TRIVIAL)
label_map_disc = pd.read_csv(LABEL_MAP_DISCOGS)
label_map_disc_ext = pd.read_csv(LABEL_MAP_DISCOGS_EXT, dtype={'discogs_wiki_url': str})
label_map_wiki = pd.read_csv(LABEL_MAP_WIKIPEDIA)
label_map_wiki_ext = pd.read_csv(LABEL_MAP_WIKIPEDIA_EXT, dtype={'discogs_wiki_url': str, 'wiki_url': str})
label_map_inte = pd.read_csv(LABEL_MAP_INTERIM)

print(label_map_triv.shape)
print(label_map_disc.shape)
print(label_map_disc_ext.shape)
print(label_map_wiki.shape)
print(label_map_wiki_ext.shape)
print(label_map_inte.shape)

print('mapping: trivial -> discogs')
label_map_disc = forward_mapping(label_map_triv, CLASS_TRIVIAL, label_map_disc, CLASS_DISCOGS)
label_map_disc_ext = forward_mapping(label_map_triv, CLASS_TRIVIAL, label_map_disc_ext, CLASS_DISCOGS)
print('mapping: discogs -> wiki')
label_map_wiki = forward_mapping(label_map_disc, CLASS_DISCOGS, label_map_wiki, CLASS_WIKIPEDIA)
label_map_wiki_ext = forward_mapping(label_map_disc, CLASS_DISCOGS, label_map_wiki_ext, CLASS_WIKIPEDIA)
print('mapping: wiki -> interim')
label_map_inte = forward_mapping(label_map_wiki, CLASS_WIKIPEDIA, label_map_inte, CLASS_INTERIM)

label_map_disc.to_csv(LABEL_MAP_DISCOGS, index=False)
label_map_disc_ext.to_csv(LABEL_MAP_DISCOGS_EXT, index=False)
label_map_wiki.to_csv(LABEL_MAP_WIKIPEDIA, index=False)
label_map_wiki_ext.to_csv(LABEL_MAP_WIKIPEDIA_EXT, index=False)
label_map_inte.to_csv(LABEL_MAP_INTERIM, index=False)
