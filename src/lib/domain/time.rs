//! Clock access and the units deadlines are measured in.
//!
//! Everything below the parser stores and compares deadlines as [`Milliseconds`]. The wire
//! speaks both units (`EXPIRE` and `EXAT` in seconds, `PEXPIREAT` and `PXAT` in millis), so
//! [`Seconds`] exists only long enough to be converted at parse time. Nothing downstream
//! has to care which verb produced a deadline.
//!
//! These live in `domain` rather than `resp` because AOF replay reaches the same
//! conversions through `Command::try_from`, and putting them in the wire layer would point
//! the dependency arrow outward.

use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use wincode::{SchemaRead, SchemaWrite};

/// A second count, straight off the wire. Convert to [`Milliseconds`] before it goes
/// anywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seconds(u64);

impl Seconds {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

/// A millisecond count: either a UNIX timestamp or a duration, depending on who made it.
///
/// Carries the `wincode` derives because it lands inside `Entry`, which is what the
/// snapshot serializes.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, SchemaWrite, SchemaRead,
)]
pub struct Milliseconds(u64);

impl Milliseconds {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(&self) -> u64 {
        self.0
    }

    /// Current UNIX time.
    ///
    /// `Duration::as_millis` returns `u128`, so this narrows, saturating at `u64::MAX`
    /// in year 584,556,019. A deadline crossing the `i64` reply boundary saturates
    /// sooner, in year 292,278,994. See the `TTL` arm of `Cache::execute`.
    ///
    /// `SystemTime` rather than `Instant` because `Instant` is process-local and these
    /// deadlines outlive the process.
    pub fn now() -> Result<Self, SystemTimeError> {
        let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        Ok(Self(u64::try_from(millis).unwrap_or(u64::MAX)))
    }

    /// Difference floored at zero, so a passed deadline reads as no time left.
    #[must_use]
    pub fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    /// Sum clamped at `u64::MAX`. Turns a relative TTL into an absolute deadline.
    #[must_use]
    pub fn saturating_add(self, other: Self) -> Self {
        Self(self.0.saturating_add(other.0))
    }

    /// Round up to whole seconds. `TTL` reports seconds, so its reply divides here; a
    /// `PTTL` arm would skip this.
    ///
    /// Up rather than down because this is used on a remaining duration: `SET k v EX 60`
    /// read back a microsecond later has 59.999s left, and Redis answers that with 60.
    /// Truncating would answer 59 and make every TTL look a second short.
    pub fn to_seconds_rounded_up(self) -> Seconds {
        Seconds(self.0.div_ceil(1000))
    }
}

/// Saturating at `u64::MAX` rather than erroring: an absurd `EXPIREAT 99999999999999999999`
/// clamps to a deadline no clock reaches, which is what was asked for anyway.
impl From<Seconds> for Milliseconds {
    fn from(value: Seconds) -> Self {
        Self(value.0.saturating_mul(1000))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_is_after_2024() {
        // 2024-01-01T00:00:00Z
        assert!(Milliseconds::now().unwrap() > Milliseconds::new(1_704_067_200_000));
    }

    #[test]
    fn now_is_millis_not_secs() {
        // A seconds value would be ~1.7e9; millis is ~1.7e12.
        assert!(Milliseconds::now().unwrap() > Milliseconds::new(1_000_000_000_000));
    }

    #[test]
    fn to_seconds_rounds_up() {
        assert_eq!(
            Milliseconds::new(59_999).to_seconds_rounded_up(),
            Seconds::new(60)
        );
        assert_eq!(
            Milliseconds::new(60_000).to_seconds_rounded_up(),
            Seconds::new(60)
        );
        assert_eq!(
            Milliseconds::new(1).to_seconds_rounded_up(),
            Seconds::new(1)
        );
        assert_eq!(
            Milliseconds::new(0).to_seconds_rounded_up(),
            Seconds::new(0)
        );
    }

    #[test]
    fn seconds_convert_to_millis() {
        assert_eq!(
            Milliseconds::from(Seconds::new(60)),
            Milliseconds::new(60_000)
        );
        assert_eq!(Milliseconds::from(Seconds::new(0)), Milliseconds::new(0));
    }

    #[test]
    fn seconds_conversion_saturates() {
        assert_eq!(
            Milliseconds::from(Seconds::new(u64::MAX)),
            Milliseconds::new(u64::MAX)
        );
    }
}
