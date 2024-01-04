// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use enum_as_inner::EnumAsInner;
use parse_display::Display;
use risingwave_common::bail;
use risingwave_common::types::{
    DataType, Datum, ScalarImpl, ScalarRefImpl, Sentinelled, ToDatumRef, ToOwnedDatum, ToText,
};
use risingwave_common::util::sort_util::{Direction, OrderType};
use risingwave_pb::expr::window_frame::{PbBound, PbExclusion};
use risingwave_pb::expr::{PbWindowFrame, PbWindowFunction};
use FrameBound::{CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding};

use super::WindowFuncKind;
use crate::aggregate::AggArgs;
use crate::Result;

#[derive(Debug, Clone)]
pub struct WindowFuncCall {
    pub kind: WindowFuncKind,
    pub args: AggArgs,
    pub return_type: DataType,
    pub frame: Frame,
}

impl WindowFuncCall {
    pub fn from_protobuf(call: &PbWindowFunction) -> Result<Self> {
        let call = WindowFuncCall {
            kind: WindowFuncKind::from_protobuf(call.get_type()?)?,
            args: AggArgs::from_protobuf(call.get_args())?,
            return_type: DataType::from(call.get_return_type()?),
            frame: Frame::from_protobuf(call.get_frame()?)?,
        };
        Ok(call)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Frame {
    pub bounds: FrameBounds,
    pub exclusion: FrameExclusion,
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.bounds)?;
        if self.exclusion != FrameExclusion::default() {
            write!(f, " {}", self.exclusion)?;
        }
        Ok(())
    }
}

impl Frame {
    pub fn rows(start: FrameBound<usize>, end: FrameBound<usize>) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion: FrameExclusion::default(),
        }
    }

    pub fn rows_with_exclusion(
        start: FrameBound<usize>,
        end: FrameBound<usize>,
        exclusion: FrameExclusion,
    ) -> Self {
        Self {
            bounds: FrameBounds::Rows(RowsFrameBounds { start, end }),
            exclusion,
        }
    }
}

impl Frame {
    pub fn from_protobuf(frame: &PbWindowFrame) -> Result<Self> {
        use risingwave_pb::expr::window_frame::PbType;
        let bounds = match frame.get_type()? {
            PbType::Unspecified => bail!("unspecified type of `WindowFrame`"),
            PbType::Rows => {
                let start = FrameBound::from_protobuf(frame.get_start()?)?;
                let end = FrameBound::from_protobuf(frame.get_end()?)?;
                FrameBounds::Rows(RowsFrameBounds { start, end })
            }
        };
        let exclusion = FrameExclusion::from_protobuf(frame.get_exclusion()?)?;
        Ok(Self { bounds, exclusion })
    }

    pub fn to_protobuf(&self) -> PbWindowFrame {
        use risingwave_pb::expr::window_frame::PbType;
        let exclusion = self.exclusion.to_protobuf() as _;
        match &self.bounds {
            FrameBounds::Rows(RowsFrameBounds { start, end }) => PbWindowFrame {
                r#type: PbType::Rows as _,
                start: Some(start.to_protobuf()),
                end: Some(end.to_protobuf()),
                exclusion,
            },
            FrameBounds::Range(RangeFrameBounds { .. }) => {
                todo!() // TODO()
            }
        }
    }
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash, EnumAsInner)]
#[display("{0}")]
pub enum FrameBounds {
    Rows(RowsFrameBounds),
    // Groups(GroupsFrameBounds),
    Range(RangeFrameBounds),
}

impl FrameBounds {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Rows(bounds) => bounds.validate(),
            Self::Range(bounds) => bounds.validate(),
        }
    }

    pub fn start_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { start, .. }) => start.is_unbounded_preceding(),
            Self::Range(RangeFrameBounds { start, .. }) => start.is_unbounded_preceding(),
        }
    }

    pub fn end_is_unbounded(&self) -> bool {
        match self {
            Self::Rows(RowsFrameBounds { end, .. }) => end.is_unbounded_following(),
            Self::Range(RangeFrameBounds { end, .. }) => end.is_unbounded_following(),
        }
    }

    pub fn is_unbounded(&self) -> bool {
        self.start_is_unbounded() || self.end_is_unbounded()
    }
}

#[derive(Display, Debug, Clone, Eq, PartialEq, Hash)]
#[display("ROWS BETWEEN {start} AND {end}")]
pub struct RowsFrameBounds {
    pub start: FrameBound<usize>,
    pub end: FrameBound<usize>,
}

impl RowsFrameBounds {
    fn validate(&self) -> Result<()> {
        FrameBound::validate_bounds(&self.start, &self.end, |_| Ok(()))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RangeFrameBounds {
    pub start: FrameBound<ScalarImpl>,
    pub end: FrameBound<ScalarImpl>,
}

impl Display for RangeFrameBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RANGE BETWEEN {} AND {}",
            self.start.for_display(),
            self.end.for_display()
        )?;
        Ok(())
    }
}

impl RangeFrameBounds {
    fn validate(&self) -> Result<()> {
        FrameBound::validate_bounds(&self.start, &self.end, |offset| {
            match offset.as_scalar_ref_impl() {
                // TODO(): use decl macro to merge with the following
                ScalarRefImpl::Int16(val) if val < 0 => {
                    bail!("frame bound offset should be non-negative, but {} is given", val);
                }
                ScalarRefImpl::Int32(val) if val < 0 => {
                    bail!("frame bound offset should be non-negative, but {} is given", val);
                }
                ScalarRefImpl::Int64(val) if val < 0 => {
                    bail!("frame bound offset should be non-negative, but {} is given", val);
                }
                // TODO(): datetime types
                _ => unreachable!("other order column data types are not supported and should be banned in frontend"),
            }
        })
    }

    /// Get the frame start for a given order column value.
    ///
    /// ## Examples
    ///
    /// For the following frames:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// ORDER BY x DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    /// ```
    ///
    /// For any CURRENT ROW with any order value, the frame start is always the first-most row, which is
    /// represented by [`Sentinelled::Smallest`].
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
    /// ```
    ///
    /// For CURRENT ROW with order value `100`, the frame start is the **FIRST** row with order value `90`.
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW
    /// ```
    ///
    /// For CURRENT ROW with order value `100`, the frame start is the **FIRST** row with order value `110`.
    pub fn frame_start_of(
        &self,
        order_value: impl ToDatumRef,
        order_type: OrderType,
    ) -> Sentinelled<Datum> {
        self.start.as_ref().bound_of(order_value, order_type)
    }

    /// Get the frame end for a given order column value. It's very similar to `frame_start_of`, just with
    /// everything on the other direction.
    pub fn frame_end_of(
        &self,
        order_value: impl ToDatumRef,
        order_type: OrderType,
    ) -> Sentinelled<Datum> {
        self.end.as_ref().bound_of(order_value, order_type)
    }

    /// Get the order value of the CURRENT ROW of the first-most frame that includes the given order value.
    ///
    /// ## Examples
    ///
    /// For the following frames:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    /// ORDER BY x DESC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    /// ```
    ///
    /// For any given order value, the first CURRENT ROW is always the first-most row, which is
    /// represented by [`Sentinelled::Smallest`].
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x ASC RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING
    /// ```
    ///
    /// For a given order value `100`, the first CURRENT ROW should have order value `90`.
    ///
    /// For the following frame:
    ///
    /// ```sql
    /// ORDER BY x DESC RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING
    /// ```
    ///
    /// For a given order value `100`, the first CURRENT ROW should have order value `110`.
    pub fn first_curr_of(
        &self,
        order_value: impl ToDatumRef,
        order_type: OrderType,
    ) -> Sentinelled<Datum> {
        self.end
            .as_ref()
            .reverse()
            .bound_of(order_value, order_type)
    }

    /// Get the order value of the CURRENT ROW of the last-most frame that includes the given order value.
    /// It's very similar to `first_curr_of`, just with everything on the other direction.
    pub fn last_curr_of(
        &self,
        order_value: impl ToDatumRef,
        order_type: OrderType,
    ) -> Sentinelled<Datum> {
        self.start
            .as_ref()
            .reverse()
            .bound_of(order_value, order_type)
    }
}

#[derive(Display, Debug, Clone, Copy, Eq, PartialEq, Hash, EnumAsInner)]
#[display(style = "TITLE CASE")]
pub enum FrameBound<T> {
    UnboundedPreceding,
    #[display("{0} PRECEDING")]
    Preceding(T),
    CurrentRow,
    #[display("{0} FOLLOWING")]
    Following(T),
    UnboundedFollowing,
}

impl<T> FrameBound<T> {
    fn offset_value(&self) -> Option<&T> {
        match self {
            UnboundedPreceding | UnboundedFollowing | CurrentRow => None,
            Preceding(offset) | Following(offset) => Some(offset),
        }
    }

    fn validate_bounds(
        start: &Self,
        end: &Self,
        offset_checker: impl Fn(&T) -> Result<()>,
    ) -> Result<()> {
        match (start, end) {
            (_, UnboundedPreceding) => bail!("frame end cannot be UNBOUNDED PRECEDING"),
            (UnboundedFollowing, _) => {
                bail!("frame start cannot be UNBOUNDED FOLLOWING")
            }
            (Following(_), CurrentRow) | (Following(_), Preceding(_)) => {
                bail!("frame starting from following row cannot have preceding rows")
            }
            (CurrentRow, Preceding(_)) => {
                bail!("frame starting from current row cannot have preceding rows")
            }
            _ => {}
        }

        for bound in [start, end] {
            if let Some(offset) = bound.offset_value() {
                offset_checker(offset)?;
            }
        }

        Ok(())
    }
}

impl<T> FrameBound<T>
where
    FrameBound<T>: Copy,
{
    fn reverse(self) -> FrameBound<T> {
        match self {
            UnboundedPreceding => UnboundedFollowing,
            Preceding(offset) => Following(offset),
            CurrentRow => CurrentRow,
            Following(offset) => Preceding(offset),
            UnboundedFollowing => UnboundedPreceding,
        }
    }
}

impl FrameBound<usize> {
    pub fn from_protobuf(bound: &PbBound) -> Result<Self> {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let offset = bound.get_offset()?;
        let bound = match offset {
            PbOffset::Integer(offset) => match bound.get_type()? {
                PbBoundType::Unspecified => bail!("unspecified type of `FrameBound<usize>`"),
                PbBoundType::UnboundedPreceding => Self::UnboundedPreceding,
                PbBoundType::Preceding => Self::Preceding(*offset as usize),
                PbBoundType::CurrentRow => Self::CurrentRow,
                PbBoundType::Following => Self::Following(*offset as usize),
                PbBoundType::UnboundedFollowing => Self::UnboundedFollowing,
            },
            PbOffset::Datum(_) => bail!("offset of `FrameBound<usize>` must be `Integer`"),
        };
        Ok(bound)
    }

    pub fn to_protobuf(&self) -> PbBound {
        use risingwave_pb::expr::window_frame::bound::PbOffset;
        use risingwave_pb::expr::window_frame::PbBoundType;

        let (r#type, offset) = match self {
            Self::UnboundedPreceding => (PbBoundType::UnboundedPreceding, PbOffset::Integer(0)),
            Self::Preceding(offset) => (PbBoundType::Preceding, PbOffset::Integer(*offset as _)),
            Self::CurrentRow => (PbBoundType::CurrentRow, PbOffset::Integer(0)),
            Self::Following(offset) => (PbBoundType::Following, PbOffset::Integer(*offset as _)),
            Self::UnboundedFollowing => (PbBoundType::UnboundedFollowing, PbOffset::Integer(0)),
        };
        PbBound {
            r#type: r#type as _,
            offset: Some(offset),
        }
    }
}

impl FrameBound<usize> {
    /// Convert the bound to sized offset from current row. `None` if the bound is unbounded.
    pub fn to_offset(&self) -> Option<isize> {
        match self {
            UnboundedPreceding | UnboundedFollowing => None,
            CurrentRow => Some(0),
            Preceding(n) => Some(-(*n as isize)),
            Following(n) => Some(*n as isize),
        }
    }

    /// View the bound as frame start, and get the number of preceding rows.
    pub fn n_preceding_rows(&self) -> Option<usize> {
        self.to_offset().map(|x| x.min(0).unsigned_abs())
    }

    /// View the bound as frame end, and get the number of following rows.
    pub fn n_following_rows(&self) -> Option<usize> {
        self.to_offset().map(|x| x.max(0) as usize)
    }
}

impl FrameBound<ScalarImpl> {
    fn as_ref(&self) -> FrameBound<ScalarRefImpl<'_>> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(offset.as_scalar_ref_impl()),
            CurrentRow => CurrentRow,
            Following(offset) => Following(offset.as_scalar_ref_impl()),
            UnboundedFollowing => UnboundedFollowing,
        }
    }

    fn for_display(&self) -> FrameBound<String> {
        match self {
            UnboundedPreceding => UnboundedPreceding,
            Preceding(offset) => Preceding(offset.as_scalar_ref_impl().to_text()),
            CurrentRow => CurrentRow,
            Following(offset) => Following(offset.as_scalar_ref_impl().to_text()),
            UnboundedFollowing => UnboundedFollowing,
        }
    }
}

impl FrameBound<ScalarRefImpl<'_>> {
    fn bound_of(self, order_value: impl ToDatumRef, order_type: OrderType) -> Sentinelled<Datum> {
        let order_value = order_value.to_datum_ref();
        match (self, order_type.direction()) {
            (UnboundedPreceding, _) => Sentinelled::Smallest,
            (UnboundedFollowing, _) => Sentinelled::Largest,
            (CurrentRow, _) => Sentinelled::Normal(order_value.to_owned_datum()),
            (Preceding(offset), Direction::Ascending)
            | (Following(offset), Direction::Descending) => {
                // should SUBTRACT the offset
                if let Some(value) = order_value {
                    let res = match (value, offset) {
                        // TODO(): use decl macro to merge with the following
                        (ScalarRefImpl::Int16(val), ScalarRefImpl::Int16(off)) => {
                            ScalarImpl::Int16(val - off)
                        }
                        (ScalarRefImpl::Int32(val), ScalarRefImpl::Int32(off)) => {
                            ScalarImpl::Int32(val - off)
                        }
                        (ScalarRefImpl::Int64(val), ScalarRefImpl::Int64(off)) => {
                            ScalarImpl::Int64(val - off)
                        }
                        // TODO(): datetime types
                        _ => unreachable!("other order column data types are not supported and should be banned in frontend"),
                    };
                    Sentinelled::Normal(Some(res))
                } else {
                    Sentinelled::Normal(None)
                }
            }
            (Following(offset), Direction::Ascending)
            | (Preceding(offset), Direction::Descending) => {
                // should ADD the offset
                if let Some(value) = order_value {
                    let res = match (value, offset) {
                        // TODO(): use decl macro to merge with the following
                        (ScalarRefImpl::Int16(val), ScalarRefImpl::Int16(off)) => {
                            ScalarImpl::Int16(val + off)
                        }
                        (ScalarRefImpl::Int32(val), ScalarRefImpl::Int32(off)) => {
                            ScalarImpl::Int32(val + off)
                        }
                        (ScalarRefImpl::Int64(val), ScalarRefImpl::Int64(off)) => {
                            ScalarImpl::Int64(val + off)
                        }
                        // TODO(): datetime types
                        _ => unreachable!("other order column data types are not supported and should be banned in frontend"),
                    };
                    Sentinelled::Normal(Some(res))
                } else {
                    Sentinelled::Normal(None)
                }
            }
        }
    }
}

#[derive(Display, Debug, Copy, Clone, Eq, PartialEq, Hash, Default, EnumAsInner)]
#[display("EXCLUDE {}", style = "TITLE CASE")]
pub enum FrameExclusion {
    CurrentRow,
    // Group,
    // Ties,
    #[default]
    NoOthers,
}

impl FrameExclusion {
    pub fn from_protobuf(exclusion: PbExclusion) -> Result<Self> {
        let excl = match exclusion {
            PbExclusion::Unspecified => bail!("unspecified type of `FrameExclusion`"),
            PbExclusion::CurrentRow => Self::CurrentRow,
            PbExclusion::NoOthers => Self::NoOthers,
        };
        Ok(excl)
    }

    pub fn to_protobuf(self) -> PbExclusion {
        match self {
            Self::CurrentRow => PbExclusion::CurrentRow,
            Self::NoOthers => PbExclusion::NoOthers,
        }
    }
}
