# Analysing playlist data to investigate the impact of record labels on music recommender systems

Author: Moritz Hübler


## General

This project contains all scripts of my master's thesis for enriching music data with record label information by
crawling Spotify, Discogs and Wikipedia. 

### Usage

1. Generate input list of either spotify track_uris or spotify album_uris
   1. For the MPD dataset this already exists: _generate_input_list_mpd.py_
   2. For the lfm-2b dataset this already exists: _generate_input_list_lfm.py_
2. Run _main.py_ script with the following optional arguments:
   1. __-d | --debug__: Flag to print additional output.
   2. __-a | --analysis__: Flag to run optional analysis after preprocessing and crawling.
   3. __-r | --reuse_label_map__: Path to an existing label map (e.g. from previous runs) which should re reused.
      The file should be in csv format and include this two columns: <_record_label_low_, _record_label_major_>
   4. __-t | --track_start__: Flag if the input list from step 1 contains track_uris instead of album_uris which is the default. 
   
If the crawler is used for different dataset, a suffix-tag can be defined in _config.py_. This appends the defined
suffix to all generated output files.

All other parameters and paths are defined in _constants.py_. See chapter [Credentials & Constants](#chaptercred) on how to set up your credentials
for Spotify and Discogs.

#### Used Setups:

For the MPD `_mpd` was set as dataset_tag in the _config.py_.
For an album based start with no existing label map the following arguments were used:
```
generate_input_list_mpd.py
main.py --analysis
```
And for the LFM-2b with `_lfm` as tag in the _config.py_, the following arguments were used,
for a track based start reusing the existing label map from the previous MPD run:
```
generate_input_list_lfm.py
main.py --track_start --reuse_label_map=../data/generated/label_map_final_mpd.csv --analysis
```

### <a id="chaptercred"></a>Credentials & Constants

In order to run the label crawler it's necessary to fill in two credential files on the top level 
of the project: 
1. _spotify_credentials.py_: Credentials of your registered application from your spotify developer account (https://developer.spotify.com/), including:
   * CLIENT_ID
   * CLIENT_SECRET
2. _discogs_credentials.py_: Credentials of your registered discogs application (https://www.discogs.com/settings/developers), including:    
   * consumer_key
   * consumer_secret
   * user_agent
   * discogs_token
   
## Structure

All necessary scripts to run the crawler are located on the top level of the _src_ folder.

The full crawler itself is split into the following folders:
1. _preprocessing_spotify_: Contains the preprocessing scripts which use Spotify to get 
the low-level record labels and copyright information. The _run_preprocessing.py_ script is used by _main.py_.
2. _label_crawler_: Contains the different crawling steps, all orchestrated by _run_crawler.py_
3. _analysis_: Scripts to run analysis on base/preprocessed and enriched datasets (for LFM-2b and MPD)
4. _recSys18_experiments_: Experiments on applying re-ranking in a post-filter step to the KAENEN submission of the recSys18 (https://www.aicrowd.com/challenges/spotify-million-playlist-dataset-challenge)
5. 

### Preprocessing (using Spotify):

Depending on the type of input (track_uris or album_uris) first the _spotify_album_crawler.py_ is used to get to from 
the track_uris to the album_uris. 


1. (_spotify_album_crawler.py_: Optional, depending on input. Get album_uri for all track_uris.)
2. _spotify_album_crawler.py_: Gather full album information including: low-level record label (=The label defined on
   Spotify), copyright information.
3. _spotify_crawler_postprocessing_: Generate sorted list of low-level record labels, sorted by occurrences descending
4. (_spotify_crawler_analysis_: Analyse the gathered low-level record information.) 

### Multi-Stage record label classification:

1. _label_mapping_trivial.py_
   * Use small dictionary of alias to map trivial low-level record labels to major-labels (e.g. Universal Music -> Universal Music Group)
2. _discogs_label_crawler.py_
   * Use discogs search engine to find parent label
   * No independent classification, but persist:
     * Keyword aggregate
     * Link to wiki-page if exists
3. _wikipedia_crawler.py_
   * Use wikipedia search engine to classify low-level label
   * Again no independent classification, but:
     * Keyword aggregate
     * Boolean if link to 'wiki/Independent_record_label' exists
4. _label_mapping_interim.py_
   * If label has wiki link to independent record label and wiki keyword aggregate <= 2 classify as Independent
   * If sum of wiki keyword aggregate is > 0.25, classify as maximum of aggregate
5. _label_copyright_classifier.py_
   * Analyze most frequent terms in copyright text of already classified low-level labels
   * Create alias dicts for major-labels manually from most frequent and decisive tokens
   * Check for already classified low-level labels if a token in its copyright body of a different major label is more frequent, if so change classification
6. _label_mapping_final.py_
   * Manual checkup, changes of already classified labels are possible
   * For all labels which are UNKN or INDI take max of discogs keyword aggregate when sum >= 0.2
   * Otherwise: If a discogs page exists = Independent, Unknown otherwise
7. (_label_classification_analysis.py_: Run analysis of gathered record-information and in which step 
   how many classifications happened.)
8. _label_classification_postprocessing.py_: Map the final classification back into the original album_uri_map or track_uri_map, depending on how the preprocessing was started

### Additional Information

A list of stopwords are used from nltk is used, for this it is necessary to run:
```
import nltk
nltk.download('stopwords')
```



