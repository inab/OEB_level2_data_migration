# OEB Level2 data migration original id naming conventions

## Benchmarking events

* [W] Original id from a benchmarking event must start with the community prefix,
  which is the community label followed by ':'.

* The community label is usually the `acronym` from the **`Community`** entry. In some cases
  where the label should be something easier or custom, it can be customized at the community
  entry level, including the key `level_2:community_label` in `_metadata` section. Example:

  ```json
  {
    "_metadata": {
      "level_2:community_label": "SIMPLELABEL"
    }
  }
  ```

## Challenges

* [W] Original id from a challenge must start with the community prefix.

* [W] Original id from a challenge must start with the benchmarking event original id,
followed by the benchmarking event original id separator,
and then should be followed by the challenge label.

* The original id separator is by default `_`. It can be customized both at the
  benchmarking event and the challenge levels, including the key
  `level_2:orig_id_separator`. Example: 

  ```json
  {
    "_metadata": {
      "level_2:orig_id_separator": ">"
    }
  }
  ```

  For instance, if it is set at the challenge level, but not at the benchmarking
  event level, the separator between the benchmarking event original id and challenge
  label would be `_`, and the separator for further content would be `>` 

* The aggregation dataset separator is by default `agg`. It can be customized both at the
  benchmarking event and the challenge levels, including the key
  `level_2:aggregation_separator`. Example: 

  ```json
  {
    "_metadata": {
      "level_2:aggregation_separator": "Aggregation"
    }
  }
  ```

* The metrics label separator is by default `+`. It can be customized both at the
  benchmarking event and the challenge levels, including the key
  `level_2:metric_separator`. Example: 

  ```json
  {
    "_metadata": {
      "level_2:metric_separator": "_vs_"
    }
  }
  ```

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
    - ... and the original id starts with the original id of the parent benchmarking event plus the
      benchmarking event original id separator, that prefix is removed and the rest is used as label.
      
    - If the benchmarking event prefix does not match, but the community prefix
      (community label plus ':') matches, then that prefix is removed and the
      rest is used as label.
      
    - Otherwise, use the original id as label.
  
  - Last resort, the challenge label is the OpenEBench challenge official id.

## Metrics

* [I] Metrics (the abstract definition) which are narrowed to specific communities due
    their definition or labelling should have their `orig_id` prefixed by the
    community prefix.
    
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
    
  - If the metrics entry has an `orig_id`, and the original id has as prefix the community prefix,
    then the metrics label is the result of removing the prefix.
  
  - In the worst cases, if the metrics entry has an `orig_id`, the original id is the metrics label.
    Else the label is the metrics id.

* Assessment entries are discarded from the aggregation process when their values are outside the limits
  of the metrics. The way a metrics entry can declare this limits is through the `representation_hints`
  block, where restrictions on one or both extremes can be declared.
  
  Next example shows how to declare a metric value should be between 0 and 1, but the lower
  value should be exclusive (i.e. discarding the 0):
  
  ```json
  {
    "representation_hints": {
      "limits": {
        "min": 0,
        "min_inclusive": false,
        "max": 1
      }
    }
  }
  ```

## Datasets

* [W] Original id from a dataset tied to a single community must start with the community prefix.

* [W] Original id from a dataset tied to a single benchmarking event must start with the benchmarking event original_id,
  followed by benchmarking event original id separator.

* [W] Original id from a dataset tied to a single challenge must start with the challenge's original_id,
  followed by challenge original id separator.

* [W] A participant dataset's original id must end with the participant label, plus '_P'.

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
    
* Participant entries, and subsequent assessment entries, are excluded (i.e. hard masked) when
  `_metadata` section from the participant dataset contains the key `level_2:exclude_participant`
  with a value which evaluates to `true`.

* [W] An assessment dataset's original id must end with the metrics label of the involved metric,
  plus challenge original id separator, plus the participant label, plus '_A'.

* [W] An aggregation dataset's original id must end with the orig_id separator,
  plus the aggregation separator plus the orig_id separator, plus labels of the involved metrics, joined by
  the challenge metric label separator (by default '+').

## TestActions

* [W] A test action original id must start with the community prefix.

* [W] A test action original id must start with the benchmarking event `orig_id`,
  followed by benchmarking event original id separator.

* [W] A test action original id must start with the challenge's `orig_id`,
  followed by challenge original id separator.

* [W] A TestEvent test action original id must start with the challenge's original id , followed by '_testEvent_' and the metrics label.

* [W] A MetricsEvent test action original id must start with the original id of the generated assessment dataset, replacing the '_A' suffix with '_MetricsEvent'.

* [W] An AggregationEvent test action original id must start with the original id of the generated aggregation dataset, followed by '_Event'.
