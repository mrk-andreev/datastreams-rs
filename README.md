# DataStreams-RS

DataStreams-RS maintain sliding data stream with fixed BUCKET_SIZE_MS for fixed HISTORY_SIZE. Data structure initialize with INITIAL_TS.

Measurements older `{max_ts} - BUCKET_SIZE_MS * HISTORY_SIZE` will be skipped. Measurements newer `{max_ts}` (initially `=INITIAL_TS`) will move data frame to future and remove old data.

Data structure maintain single value for each bucket. New values for same bucket will override previous one.

Supported operations:

- Initialization
- Insert new value
- Iteration
- Boundary fetch
- Aggregation with custom `AGGREGATION_MS` (expect that `AGGREGATION_MS >> BUCKET_SIZE_MS`)
  - aggregation_min
  - aggregation_max
  - aggregation_avg
  - `custom Fn(&Vec<f64>) -> f64`

## Installation

```
cargo install datastreams-rs
```

## Usage

```rust
const HISTORY_SIZE: usize = 10;
const BUCKET_SIZE_MS: u64 = 1_000;
const INITIAL_TS: u64 = 1_714_321_497_981;

fn main() {
    let mut data_stream: DataStream = create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

    // insert new values
    data_stream.add_value(INITIAL_TS + 0 * BUCKET_SIZE_MS, 1f64);
    data_stream.add_value(INITIAL_TS + 1 * BUCKET_SIZE_MS, 2f64);
    data_stream.add_value(INITIAL_TS + 2 * BUCKET_SIZE_MS, 2f64);
    data_stream.add_value(INITIAL_TS + 3 * BUCKET_SIZE_MS, 2f64);

    // iterate over data stream
    for data_stream_value in data_stream.into_iter() {
        let _t = data_stream_value.timestamp;
        let _v = data_stream_value.value;

        println!("{_t}={_v}");
    }

    // aggregation
    const AGGREGATION_MS: u64 = 2 * BUCKET_SIZE_MS;
    let mut out: Vec<f64> = Vec::new();
    data_stream.agg(aggregation_min, AGGREGATION_MS, &mut out);
    data_stream.agg(aggregation_max, AGGREGATION_MS, &mut out);
    data_stream.agg(aggregation_avg, AGGREGATION_MS, &mut out);
}
```

## Release notes

### 0.3.0

- Remove generics data streams configuration

### 0.2.0

- Add aggregations

### 0.1.0

- Initial release


## For developers

Evaluate this commands before commit:

```
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
```

## Author

Mark Andreev ( [linkedin.com/in/mrk-andreev](https://www.linkedin.com/in/mrk-andreev/); [mrkandreev.name](https://mrkandreev.name/) ) 

