import sys
import pandas as pd

from src import constants

DEBUG = constants.DEBUG

INPUT_PATH = constants.LABEL_MAP
OUTPUT_PATH = constants.LABEL_MAP_TRIVIAL

# columns for label map
RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
CLASS_TRIVIAL = constants.CLASS_TRIVIAL

FINAL_UNIV = constants.FINAL_UNIV
FINAL_SONY = constants.FINAL_SONY
FINAL_WARN = constants.FINAL_WARN
FINAL_INDI = constants.FINAL_INDI
FINAL_UNKN = constants.FINAL_UNKN

UNIV_ALIAS = constants.UNIV_ALIAS
SONY_ALIAS = constants.SONY_ALIAS
WARN_ALIAS = constants.WARN_ALIAS
INDI_ALIAS = constants.INDI_ALIAS
UNKN_ALIAS = constants.UNKN_ALIAS

MATCHING_UNIV_CSV = '../data/generated/matching_univ_alias.csv'
MATCHING_SONY_CSV = '../data/generated/matching_sony_alias.csv'
MATCHING_WARN_CSV = '../data/generated/matching_warn_alias.csv'
MATCHING_INDI_CSV = '../data/generated/matching_indi_alias.csv'
MATCHING_UNKN_CSV = '../data/generated/matching_unkn_alias.csv'


def run_trivial_label_mapping():
    if DEBUG: print('Read label map from', INPUT_PATH)
    df_label_map = pd.read_csv(INPUT_PATH)

    if DEBUG: print('Do trivial mapping')
    # map trivial low level record labels via alias lists to high level record labels
    df_label_map.loc[df_label_map[RECORD_LABEL_LOW].str.contains(UNIV_ALIAS, case=False, na=False), CLASS_TRIVIAL] = FINAL_UNIV
    df_label_map.loc[df_label_map[RECORD_LABEL_LOW].str.contains(SONY_ALIAS, case=False, na=False), CLASS_TRIVIAL] = FINAL_SONY
    df_label_map.loc[df_label_map[RECORD_LABEL_LOW].str.contains(WARN_ALIAS, case=False, na=False), CLASS_TRIVIAL] = FINAL_WARN
    df_label_map.loc[df_label_map[RECORD_LABEL_LOW].str.contains(INDI_ALIAS, case=False, na=False), CLASS_TRIVIAL] = FINAL_INDI
    df_label_map.loc[df_label_map[RECORD_LABEL_LOW].str.fullmatch(UNKN_ALIAS, case=False, na=False), CLASS_TRIVIAL] = FINAL_UNKN
    df_label_map.loc[df_label_map[RECORD_LABEL_LOW].isna(), [RECORD_LABEL_LOW, CLASS_TRIVIAL]] = [FINAL_UNKN, FINAL_UNKN]

    if DEBUG: print('Save trivial mapping output to', OUTPUT_PATH)
    df_label_map.to_csv(OUTPUT_PATH, index=False)


def main(debug=None):
    global DEBUG

    print()
    print('##########################################################')
    print('###          CRAWLER STEP 1: TRIVIAL MAPPING           ###')
    print('##########################################################')

    if debug is not None:
        DEBUG = debug

    run_trivial_label_mapping()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Keyboard Interrupt')
        sys.exit(-1)
