# OEB Level2 data migration original id naming conventions

## Benchmarking events

[W] Original id from a benchmarking event must start with the community acronym, followed by ':'.

## Challenges

[W] Original id from a challenge must start with the community acronym, followed by ':'.

[W] Original id from a challenge must start with the benchmarking event original id, followed by '_', and then should be followed by the challenge label.

* The challenge label is derived using next rules, by their precedence:

  - If the challenge has a `_metadata` section with the key `level_2:challenge_id`, then that value is the challenge label. Example:
  
    ```json
    {
      "_metadata": {
        "level_2:challenge_id": "LABBIE"
      }
    }
    ```
    
  - If the challenge has an acronym, then that value is the challenge label.
  
  - If the challenge has an original id:
    - ... and the original id starts with the original id of the parent benchmarking event plus `_`,
      that prefix is removed and the rest is used as label.
      
    - If the benchmarking event prefix does not match, but the community prefix
      (community acronym plus ':') matches, then that prefix is removed and the
      rest is used as label.
      
    - Otherwise, use the original id as label.
  
  - Last resort, the challenge label is the OpenEBench challenge official id.

## Metrics

[I] Metrics (the abstract definition) which are narrowed to specific communities due
    their definition or labelling should have their original_id prefixed by the
    community's acronym plus ':'.
    
* Metrics label is obtained from the corresponding metrics entry through next rules,
  by their precedence:
  
  - If the metrics entry has a `_metadata` section with the key `level_2:metric_id`,
    then that value is the metrics label. Example:
    
    ```json
    {
      "_metadata": {
        "level_2:metric_id": "Recall"
      }
    }
    ```
    
  - If the metrics entry has an original_id, and the original id has as prefix the community acronym
    plus ':', then the metrics label is the result of removing the prefix.
  
  - In the worst cases, if the metrics entry has an original_id, the original id is the metrics label.
    Else the label is the metrics id.

## Datasets

[W] Original id from a dataset tied to a single community must start with the community acronym, followed by ':'.

[W] Original id from a dataset tied to a single benchmarking event must start with the benchmarking event original_id, followed by '_'.

[W] Original id from a dataset tied to a single challenge must start with the challenge's original_id, followed by '_'.

[W] A participant dataset's original id must end with the participant label, plus '_P'.

* A participant label for an assessment is obtained from the corresponding participant dataset through next rules, by their precedence:

  - If the participant dataset has a `_metadata` section with the key `level_2:participant_id`,
    then that value is the participant label. Example:
    
    ```json
    {
      "_metadata": {
        "level_2:participant_id": "Participant 2"
      }
    }
    ```
  
  - Any assessment dataset pointing to the participant dataset can also have a `_metadata` section
    with the keys `level_2:participant_id`, and also `level_2:metric_id`.
    
    ```json
    {
      "_metadata": {
        "level_2:participant_id": "Participant 2",
        "level_2:metric_id": "F1-Score"
      }
    }
    ```
  
  - Unless it is guessed by other means, the last chance comes from reversing the
    process applied to generate the original_id of the participant dataset.
    
[W] An assessment dataset's original id must end with the metrics label of the involved metric, plus '_', plus the participant label, plus '_A'.

[W] An aggregation dataset's original id must end with 'agg_', plus the labels of the involved metrics, joined by '+'.

## TestActions

[W] A test action original id must start with the community acronym, followed by ':'.

[W] A test action original id must start with the benchmarking event original_id, followed by '_'.

[W] A test action original id must start with the challenge's original_id, followed by '_'.

[W] A TestEvent test action original id must start with the challenge's original id , followed by '_testEvent_' and the metrics label.

[W] A MetricsEvent test action original id must start with the original id of the generated assessment dataset, replacing the '_A' suffix with '_MetricsEvent'.

[W] An AggregationEvent test action original id must start with the original id of the generated aggregation dataset, followed by '_Event'.
