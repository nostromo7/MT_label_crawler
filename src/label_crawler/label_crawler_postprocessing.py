import pandas as pd
from src import constants

LABEL_TO_MAJOR = constants.LABEL_MAP_FINAL
ALBUM_TO_LABEL = constants.ALBUM_URIS_WITH_LABEL_LOW
TRACK_TO_ALBUM = constants.OUTPUT_TRACK_URIS

ALBUM_URIS_ENRICHED = constants.ALBUM_URIS_ENRICHED
TRACK_URIS_ENRICHED = constants.TRACK_URIS_ENRICHED

RECORD_LABEL_LOW = constants.RECORD_LABEL_LOW
RECORD_LABEL_MAJOR = constants.RECORD_LABEL_MAJOR
ALBUM_URI = constants.ALBUM_URI
TRACK_URI = constants.TRACK_URI
OCC = constants.OCC

FINAL_INDI = constants.FINAL_INDI


def create_output_lists(track_start=False):
    df_label_to_major = pd.read_csv(LABEL_TO_MAJOR)[[RECORD_LABEL_LOW, RECORD_LABEL_MAJOR]]
    df_album_to_label = pd.read_csv(ALBUM_TO_LABEL)

    if RECORD_LABEL_MAJOR in df_album_to_label.columns:
        df_album_to_label = df_album_to_label.drop([RECORD_LABEL_MAJOR], axis=1)

    df_album_to_label = df_album_to_label.merge(df_label_to_major.drop_duplicates(subset=[RECORD_LABEL_LOW]), on=[RECORD_LABEL_LOW], how='left')

    falsey_values = ['N/A', 'n/a', 'NA', 'null']
    df_album_to_label.loc[
        (df_album_to_label[RECORD_LABEL_LOW].isna() | df_album_to_label[RECORD_LABEL_LOW].isin(falsey_values)),
            [RECORD_LABEL_LOW, RECORD_LABEL_MAJOR]] = [FINAL_INDI, FINAL_INDI]

    df_album_to_label.to_csv(ALBUM_URIS_ENRICHED, index=False)

    if track_start:
        df_track_to_album = pd.read_csv(TRACK_TO_ALBUM)
        df_track_to_album = df_track_to_album.merge(df_album_to_label.drop([OCC], axis=1), on=[ALBUM_URI], how='inner')
        df_track_to_album.to_csv(TRACK_URIS_ENRICHED, index=False)


def main(track_start=False):
    create_output_lists(track_start)


if __name__ == "__main__":
    main()