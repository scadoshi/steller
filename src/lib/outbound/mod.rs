//! The driven adapters the domain reaches through its ports.
//!
//! Just [`persister`] so far, the
//! [`CacheRepository`](crate::domain::ports::CacheRepository) impl that puts the cache on
//! disk. The RESP codec lives in [`resp`](crate::resp) instead of here, since both sides
//! use it.

pub mod persister;
