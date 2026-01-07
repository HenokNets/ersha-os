#[allow(dead_code)]
use ersha_core::{DeviceId, DeviceKind, DeviceState, DispatcherState, H3Cell};

use jiff;
use std::{collections::HashSet, ops::RangeInclusive};

pub enum SortBy {
    Id,
    ProvisionAt,
    Location,
    Manufacturer,
    State,
}

pub enum SortOrder {
    Asc,
    Desc,
}

pub struct Pagination {
    pub offset: usize,
    pub limit: Option<usize>,
}

pub struct QueryOptions<F> {
    pub filter: F,
    pub sort_by: Option<SortBy>,
    pub sort_order: SortOrder,
    pub pagination: Pagination,
}

#[derive(Default)]
pub struct DeviceFilter {
    pub ids: Option<HashSet<DeviceId>>,
    pub states: Option<HashSet<DeviceState>>,
    pub kinds: Option<HashSet<DeviceKind>>,
    pub locations: Option<HashSet<H3Cell>>,
    pub provisioned_after: Option<jiff::Timestamp>,
    pub provisioned_before: Option<jiff::Timestamp>,
    pub sensor_count: Option<RangeInclusive<usize>>,
    pub manufacturer_pattern: Option<String>,
}

#[derive(Default)]
pub struct DispatcherFilter {
    pub states: Option<HashSet<DispatcherState>>,
    pub locations: Option<HashSet<H3Cell>>,
}
