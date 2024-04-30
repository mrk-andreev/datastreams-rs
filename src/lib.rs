#![no_std]

extern crate alloc;

use alloc::vec::Vec;
use core::cmp::max;
use core::ops::Index;

pub struct DataStream {
    lo_ts: u64,
    hi_ts: u64,

    history_size: usize,
    granularity_ms: u64,

    values: Vec<f64>,
    exists: Vec<bool>,
    threshold_index: usize,
}

#[must_use]
pub fn create_data_stream(hi_ts: u64, history_size: usize, granularity_ms: u64) -> DataStream {
    let history_ms = (history_size as u64) * granularity_ms;

    let mut values: Vec<f64> = Vec::new();
    values.resize(history_size, 0f64);
    let mut exists: Vec<bool> = Vec::new();
    exists.resize(history_size, false);

    DataStream {
        hi_ts: max(hi_ts, history_ms),
        lo_ts: if history_ms >= hi_ts {
            0
        } else {
            hi_ts - history_ms
        },

        history_size,
        granularity_ms,

        values,
        exists,

        threshold_index: history_size - 1,
    }
}

#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn aggregation_avg(batch: &Vec<f64>) -> f64 {
    if batch.is_empty() {
        return f64::NAN;
    }

    let mut sum: f64 = 0f64;
    for x in batch {
        sum += x;
    }

    sum / (batch.len() as f64)
}

#[must_use]
pub fn aggregation_max(batch: &Vec<f64>) -> f64 {
    if batch.is_empty() {
        return f64::NAN;
    }

    let mut max_value = batch.index(0);

    for x in batch {
        if x > max_value {
            max_value = x;
        }
    }

    *max_value
}

#[must_use]
pub fn aggregation_min(batch: &Vec<f64>) -> f64 {
    if batch.is_empty() {
        return f64::NAN;
    }

    let mut min_value = batch.index(0);

    for x in batch {
        if x < min_value {
            min_value = x;
        }
    }

    *min_value
}

pub trait DataStreamAggregations {
    fn agg<F>(&mut self, func: F, aggregation_ms: u64, out: &mut Vec<f64>)
    where
        F: Fn(&Vec<f64>) -> f64;
}

impl DataStreamAggregations for DataStream {
    fn agg<F>(&mut self, func: F, aggregation_ms: u64, out: &mut Vec<f64>)
    where
        F: Fn(&Vec<f64>) -> f64,
    {
        out.clear();

        let mut buf: Vec<f64> = Vec::new();
        let mut local_offset_ts: u64 = 0;
        let mut pos: usize = (self.threshold_index + 1) % self.history_size;
        let mut terminated = false;

        loop {
            if terminated {
                return;
            }

            if pos == self.threshold_index {
                terminated = true;
            }

            if self.exists[pos] {
                buf.push(self.values[pos]);
            }

            local_offset_ts += self.granularity_ms;
            if local_offset_ts >= aggregation_ms {
                if buf.is_empty() {
                    out.push(f64::NAN);
                } else {
                    out.push(func(&buf));
                }
                buf.clear();
                local_offset_ts = 0;
            }

            pos = (pos + 1) % self.history_size;
        }
    }
}

pub trait DataStreamOperations {
    fn add_value(&mut self, ts: u64, value: f64);
    fn max_timestamp(&mut self) -> u64;
    fn max_value(&mut self) -> f64;
    fn last_timestamp(&mut self) -> u64;
    fn last_value(&mut self) -> f64;
    fn value_counts(&mut self) -> usize;
}

#[allow(dead_code)]
pub struct DataStreamValue {
    timestamp: u64,
    value: f64,
}

pub struct DataStreamIterMut<'a> {
    terminated: bool,
    pos: usize,
    offset: u64,
    data: &'a DataStream,
}

impl DataStream {
    #[allow(dead_code)]
    fn iter(&self) -> DataStreamIterMut {
        <&Self as IntoIterator>::into_iter(self)
    }
}

impl Iterator for DataStreamIterMut<'_> {
    type Item = DataStreamValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.terminated {
            return None;
        }

        loop {
            if self.terminated {
                return None;
            }

            if self.pos == self.data.threshold_index {
                self.terminated = true;
            }

            if self.data.exists[self.pos] {
                let timestamp = self.data.lo_ts + self.data.granularity_ms * (self.offset + 1);
                let value = self.data.values[self.pos];
                self.offset += 1;
                self.pos = (self.pos + 1) % self.data.history_size;

                return Option::from(DataStreamValue { timestamp, value });
            }

            self.offset += 1;
            self.pos = (self.pos + 1) % self.data.history_size;
        }
    }
}

impl<'a> IntoIterator for &'a DataStream {
    type Item = DataStreamValue;
    type IntoIter = DataStreamIterMut<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DataStreamIterMut {
            terminated: false,
            pos: (self.threshold_index + 1) % self.history_size,
            offset: 0,
            data: self,
        }
    }
}

impl DataStreamOperations for DataStream {
    fn add_value(&mut self, ts: u64, value: f64) {
        if ts < self.lo_ts {
            return;
        }

        if ts > self.hi_ts {
            let buckets_shift = (ts - self.hi_ts) / self.granularity_ms;
            if buckets_shift > 0 {
                let shift_value = buckets_shift * self.granularity_ms;
                let buckets_shift = usize::try_from(buckets_shift).unwrap();
                for i in 0..buckets_shift {
                    let offset = (i + self.threshold_index + 1) % self.history_size;
                    self.exists[offset] = false;
                }
                self.lo_ts += shift_value;
                self.hi_ts += shift_value;
                self.threshold_index = (self.threshold_index + buckets_shift) % self.history_size;
            }
        }

        let offset = ts - self.lo_ts;
        let bucket = offset / self.granularity_ms;

        let bucket: usize = usize::try_from(bucket).unwrap();
        let bucket = (bucket + self.threshold_index) % self.history_size;

        self.values[bucket] = value;
        self.exists[bucket] = true;
    }

    fn max_timestamp(&mut self) -> u64 {
        self.hi_ts
    }

    fn max_value(&mut self) -> f64 {
        let mut is_exists = false;
        let mut value = 0f64;

        for i in 0..self.history_size {
            if self.exists[i] && (!is_exists || self.values[i] > value) {
                is_exists = true;
                value = self.values[i];
            }
        }

        if is_exists {
            value
        } else {
            f64::NAN
        }
    }

    fn last_timestamp(&mut self) -> u64 {
        let mut offset = 0;
        let mut index = self.threshold_index % self.history_size;

        loop {
            if self.exists[index] {
                return self.hi_ts - self.granularity_ms * offset;
            }

            offset += 1;
            index = if index == 0 {
                self.history_size - 1
            } else {
                index - 1
            };
            if index == self.threshold_index {
                break;
            }
        }

        0
    }

    fn last_value(&mut self) -> f64 {
        for i in 0..self.history_size {
            let index = (i + self.threshold_index) % self.history_size;
            if self.exists[index] {
                return self.values[index];
            }
        }

        f64::NAN
    }

    fn value_counts(&mut self) -> usize {
        let mut value_counts: usize = 0;

        for is_exists in self.exists.iter() {
            if *is_exists {
                value_counts += 1;
            }
        }

        value_counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;
    use core::iter::zip;
    use core::ops::Index;

    const HISTORY_SIZE: usize = 10;
    const BUCKET_SIZE_MS: u64 = 1_000;
    const INITIAL_TS: u64 = 1_714_321_497_981;

    #[test]
    fn should_skip_add_value_for_too_late_values() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        data_stream.add_value(42u64, 1f64);

        assert_eq!(INITIAL_TS, data_stream.max_timestamp());
    }

    #[test]
    fn should_use_zero_as_marker_for_no_values_in_data_stream() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        assert_eq!(0u64, data_stream.last_timestamp());
    }

    #[test]
    fn should_add_value_inside_observation_window() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);
        let next_ts = INITIAL_TS;
        let next_value = 3f64;

        data_stream.add_value(next_ts, next_value);

        assert_eq!(INITIAL_TS, data_stream.max_timestamp());
        assert_eq!(0, next_ts - data_stream.last_timestamp());
        assert_eq!(next_ts, data_stream.last_timestamp());
        assert_eq!(next_value, data_stream.last_value());
    }

    #[test]
    fn should_return_last_value() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        data_stream.add_value(INITIAL_TS, 3f64);
        data_stream.add_value(INITIAL_TS - BUCKET_SIZE_MS, 2f64);
        data_stream.add_value(INITIAL_TS - 2 * BUCKET_SIZE_MS, 1f64);

        assert_eq!(3f64, data_stream.last_value());
    }

    #[test]
    fn should_evaluate_max_value() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        data_stream.add_value(INITIAL_TS - BUCKET_SIZE_MS, 1f64);
        data_stream.add_value(INITIAL_TS - 2 * BUCKET_SIZE_MS, 3f64);
        data_stream.add_value(INITIAL_TS - 3 * BUCKET_SIZE_MS, 2f64);

        assert_eq!(3f64, data_stream.max_value());
    }

    #[test]
    fn should_evaluate_max_value_nan_if_empty_data_stream() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        assert!(f64::is_nan(data_stream.max_value()));
    }

    #[test]
    fn should_process_window_shift() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        data_stream.add_value(INITIAL_TS, 1f64);
        data_stream.add_value(INITIAL_TS - BUCKET_SIZE_MS, 0f64);
        data_stream.add_value(INITIAL_TS, 1f64);
        data_stream.add_value(INITIAL_TS + BUCKET_SIZE_MS, 2f64);
        data_stream.add_value(INITIAL_TS + 2 * BUCKET_SIZE_MS, 3f64);

        assert_eq!(3f64, data_stream.last_value());
        assert_eq!(
            0,
            INITIAL_TS + 2 * BUCKET_SIZE_MS - data_stream.last_timestamp()
        );
        assert_eq!(
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
            data_stream.last_timestamp()
        );
    }

    #[test]
    fn should_count_existed_values_without_shift() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - 3 * BUCKET_SIZE_MS,
            INITIAL_TS - 2 * BUCKET_SIZE_MS,
            INITIAL_TS - BUCKET_SIZE_MS,
            INITIAL_TS,
        ];
        let values = vec![1f64, 2f64, 3f64, 4f64];

        assert_eq!(ts.len(), values.len());

        let mut offset: usize = 0;
        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
            offset += 1;
            assert_eq!(offset, data_stream.value_counts());
        });

        assert_eq!(ts.len(), data_stream.value_counts());
    }

    #[test]
    fn should_count_existed_values_with_shift() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - 2 * BUCKET_SIZE_MS,
            INITIAL_TS - BUCKET_SIZE_MS,
            INITIAL_TS,
            INITIAL_TS + BUCKET_SIZE_MS,
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
        ];
        let values = vec![1f64, 2f64, 3f64, 4f64, 5f64];

        assert_eq!(ts.len(), values.len());

        let mut offset: usize = 0;
        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
            offset += 1;
            assert_eq!(offset, data_stream.value_counts());
        });

        assert_eq!(ts.len(), data_stream.value_counts());
    }

    #[test]
    fn should_return_values_using_iterator() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - 2 * BUCKET_SIZE_MS,
            INITIAL_TS,
            INITIAL_TS + BUCKET_SIZE_MS,
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
        ];
        let values = vec![1f64, 2f64, 3f64, 4f64];

        assert_eq!(ts.len(), values.len());

        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
        });

        for (offset, data_stream_value) in data_stream.into_iter().enumerate() {
            let _t = data_stream_value.timestamp;
            let _v = data_stream_value.value;

            assert_eq!(values[offset], data_stream_value.value);
            assert_eq!(ts[offset], data_stream_value.timestamp);
        }
    }

    #[test]
    fn should_use_iterator() {
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - 2 * BUCKET_SIZE_MS,
            INITIAL_TS,
            INITIAL_TS + BUCKET_SIZE_MS,
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
        ];
        let values = vec![1f64, 2f64, 3f64, 4f64];

        assert_eq!(ts.len(), values.len());

        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
        });

        for (offset, data_stream_value) in data_stream.iter().enumerate() {
            let _t = data_stream_value.timestamp;
            let _v = data_stream_value.value;

            assert_eq!(values[offset], data_stream_value.value);
            assert_eq!(ts[offset], data_stream_value.timestamp);
        }
    }

    #[test]
    fn should_evaluate_avg_aggregation() {
        const LOCAL_HISTORY_SIZE: usize = 4;
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, LOCAL_HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - BUCKET_SIZE_MS,
            INITIAL_TS,
            INITIAL_TS + BUCKET_SIZE_MS,
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
        ];
        let values = vec![2f64, 2f64, 3f64, 3f64];
        assert_eq!(ts.len(), values.len());
        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
        });

        const AGGREGATION_MS: u64 = 2 * BUCKET_SIZE_MS;
        let mut out: Vec<f64> = Vec::new();
        data_stream.agg(aggregation_avg, AGGREGATION_MS, &mut out);

        assert_eq!(2usize, out.len());
        assert_eq!(2f64, *out.index(0));
        assert_eq!(3f64, *out.index(1));
    }

    #[test]
    fn should_evaluate_max_aggregation() {
        const LOCAL_HISTORY_SIZE: usize = 4;
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, LOCAL_HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - BUCKET_SIZE_MS,
            INITIAL_TS,
            INITIAL_TS + BUCKET_SIZE_MS,
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
        ];
        let values = vec![5f64, 2f64, 3f64, 10f64];
        assert_eq!(ts.len(), values.len());
        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
        });

        const AGGREGATION_MS: u64 = 2 * BUCKET_SIZE_MS;
        let mut out: Vec<f64> = Vec::new();
        data_stream.agg(aggregation_max, AGGREGATION_MS, &mut out);

        assert_eq!(2usize, out.len());
        assert_eq!(5f64, *out.index(0));
        assert_eq!(10f64, *out.index(1));
    }

    #[test]
    fn should_evaluate_min_aggregation() {
        const LOCAL_HISTORY_SIZE: usize = 4;
        let mut data_stream: DataStream =
            create_data_stream(INITIAL_TS, LOCAL_HISTORY_SIZE, BUCKET_SIZE_MS);

        let ts = vec![
            INITIAL_TS - BUCKET_SIZE_MS,
            INITIAL_TS,
            INITIAL_TS + BUCKET_SIZE_MS,
            INITIAL_TS + 2 * BUCKET_SIZE_MS,
        ];
        let values = vec![5f64, 2f64, 3f64, 10f64];
        assert_eq!(ts.len(), values.len());
        zip(ts.clone(), values.clone()).for_each(|(t, y)| {
            data_stream.add_value(t, y);
        });

        const AGGREGATION_MS: u64 = 2 * BUCKET_SIZE_MS;
        let mut out: Vec<f64> = Vec::new();
        data_stream.agg(aggregation_min, AGGREGATION_MS, &mut out);

        assert_eq!(2usize, out.len());
        assert_eq!(2f64, *out.index(0));
        assert_eq!(3f64, *out.index(1));
    }
}
