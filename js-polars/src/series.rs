use polars_core::prelude::*;
use std::ops::{BitAnd, BitOr};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = Series)]
pub struct JsSeries {
    series: Series,
}

impl From<Series> for JsSeries {
    fn from(series: Series) -> Self {
        Self { series }
    }
}

#[wasm_bindgen]
impl JsSeries {
    #[wasm_bindgen(constructor, js_name = newFlt)]
    pub fn new_flt(name: &str, values: &[f64]) -> Self {
        Self {
            series: Series::new(name, values),
        }
    }

    #[wasm_bindgen(constructor, js_name = newInt)]
    pub fn new_int(name: &str, values: &[i64]) -> Self {
        Self {
            series: Series::new(name, values),
        }
    }

    #[wasm_bindgen(constructor, js_name = newStr)]
    pub fn new_str(name: &str, values: Box<[JsValue]>) -> Self {
        let ca: Utf8Chunked = NewChunkedArray::<_, String>::new_from_iter(
            name,
            values.iter().map(|v| v.as_string().unwrap()),
        );
        Self { series: ca.into() }
    }

    #[wasm_bindgen(method)]
    pub fn rechunk(&mut self, in_place: bool) -> Option<JsSeries> {
        let series = self.series.rechunk();
        if in_place {
            self.series = series;
            None
        } else {
            Some(series.into())
        }
    }

    #[wasm_bindgen(method)]
    pub fn bitand(&self, other: &JsSeries) -> Self {
        let s = self
            .series
            .bool()
            .expect("boolean")
            .bitand(other.series.bool().expect("boolean"))
            .into_series();
        s.into()
    }

    #[wasm_bindgen(method)]
    pub fn bitor(&self, other: &JsSeries) -> Self {
        let s = self
            .series
            .bool()
            .expect("boolean")
            .bitor(other.series.bool().expect("boolean"))
            .into_series();
        s.into()
    }

    #[wasm_bindgen(method)]
    pub fn cum_sum(&self, reverse: bool) -> Self {
        self.series.cum_sum(reverse).into()
    }

    #[wasm_bindgen(method)]
    pub fn cum_max(&self, reverse: bool) -> Self {
        self.series.cum_max(reverse).into()
    }

    #[wasm_bindgen(method)]
    pub fn cum_min(&self, reverse: bool) -> Self {
        self.series.cum_min(reverse).into()
    }

    #[wasm_bindgen(method)]
    pub fn chunk_lengths(&self) -> Vec<usize> {
        self.series.chunk_lengths().clone()
    }

    #[wasm_bindgen(method)]
    pub fn name(&self) -> String {
        self.series.name().into()
    }

    #[wasm_bindgen(method)]
    pub fn rename(&mut self, name: &str) {
        self.series.rename(name);
    }

    #[wasm_bindgen(method)]
    pub fn mean(&self) -> Option<f64> {
        self.series.mean()
    }

    #[wasm_bindgen(method)]
    pub fn n_chunks(&self) -> usize {
        self.series.n_chunks()
    }

    #[wasm_bindgen(method)]
    pub fn limit(&self, num_elements: usize) -> Self {
        let series = self.series.limit(num_elements);
        series.into()
    }
}
