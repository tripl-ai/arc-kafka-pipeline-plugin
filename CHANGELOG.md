# Change Log

## 1.10.1

- add `strict` boolean attribute (default `true`) to `KafkaExtract` to throw error when `sum(endOffset) - sum(startOffset)` does not match extracted dataset count. This will not work with topics with `cleanup.policy='compact'`.

## 1.10.0

- add any values set in `params` will be added to the `KafkaConsumer`/`KafkaProducer` parameters for `KafkaExtract` and `KafkaLoad`.
- **FIX** logic error in calculation of number of consumed records to be off by `numPartitions`. This defect would result in the inability to consume from topics which are actively receiving records.
- **FIX** `maxPollRecords` was not being used and set to Kafka default value of `500`.

## 1.9.0

- add `timestampType` column (`integer`) to match Spark Kafka connector.

## 1.8.0

- update to Arc 3.4.0

## 1.7.0

- update to Arc 3.2.0

## 1.6.0

- add snippets and documentation links to implement `JupyterCompleter`.

## 1.5.0

- update to Arc 3.0.0

## 1.4.0

- update to Kafka 2.4.1

**NOTE** This is the last release supporting `Scala 2.11` given the release of `Spark 3.0` which only supports `Scala 2.12`.

## 1.3.0

- update to Kafka 2.3.1
- update to Arc 2.10.0

## 1.2.0

- update to Spark 2.4.5
- update to Arc 2.8.0
- update to Scala 2.12.10

## 1.1.0

- change `KafkaExtract` to base records to extract based on offsets at start of execution rather than waiting polling to record no records in a time window.
- improved logging for `KafkaExtract`

## 1.0.1

- update to Spark 2.4.4
- update to Arc 2.0.1
- update to Scala 2.12.9

## 1.0.0

- initial release.
