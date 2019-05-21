//! Utility macros used as helpers for common code patterns.
#[doc(hidden)]
#[macro_export]
macro_rules! unpack {
    ($ty:expr) => {
        match $ty {
            Ok(o) => o,
            Err(e) => {
                let err = future::err(e);
                return Box::new(err);
            }
        }
    };
}
