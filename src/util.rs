pub mod cpu;
pub mod triple;

use std::ops::Deref;

pub type HashMap<K, T> = std::collections::HashMap<K, T, ahash::RandomState>;
pub type HashSet<T> = std::collections::HashSet<T, ahash::RandomState>;
pub type IndexMap<K, T> = indexmap::map::IndexMap<K, T, ahash::RandomState>;
pub type IndexSet<T> = indexmap::set::IndexSet<T, ahash::RandomState>;

#[macro_export]
macro_rules! fn_expr {
    ($return_type:ty : $body:expr) => {
        (|| -> $return_type { $body })()
    };
    ($body:expr) => {
        (|| $body)()
    };
}

#[macro_export]
macro_rules! hashmap {
    ($($key:expr => $value:expr),* $(,)?) => {
        {
            let mut _map = crate::util::HashMap::default();
            $(
                let _ = _map.insert($key, $value);
            )*
            _map
        }
    };
}

#[macro_export]
macro_rules! indexmap {
    ($($key:expr => $value:expr),* $(,)?) => {
        {
            let mut _map = crate::util::IndexMap::default();
            $(
                let _ = _map.insert($key, $value);
            )*
            _map
        }
    };
}

#[macro_export]
macro_rules! hashset {
    ($($value:expr),* $(,)?) => {
        {
            let mut _set = crate::util::HashSet::default();
            $(
                let _ = _set.insert($value);
            )*
            _set
        }
    };
}

#[macro_export]
macro_rules! indexset {
    ($($value:expr),* $(,)?) => {
        {
            let mut _set = crate::util::IndexSet::default();
            $(
                let _ = _set.insert($value);
            )*
            _set
        }
    };
}

#[macro_export]
macro_rules! some_or_return {
    ($body:expr, $return_fn:expr) => {
        match $body {
            Some(r) => r,
            None => {
                return $return_fn();
            }
        }
    };
    ($body:expr) => {
        match $body {
            Some(r) => r,
            None => {
                return;
            }
        }
    };
}

#[macro_export]
macro_rules! ok_or_return {
    ($body:expr, $return_fn:expr) => {
        match $body {
            Ok(r) => r,
            Err(_) => {
                return $return_fn();
            }
        }
    };
    ($body:expr) => {
        match $body {
            Ok(r) => r,
            Err(_) => {
                return;
            }
        }
    };
}

#[macro_export]
macro_rules! some_or_continue {
    ($body:expr) => {
        match $body {
            Some(r) => r,
            None => {
                continue;
            }
        }
    };
}

#[macro_export]
macro_rules! ok_or_continue {
    ($body:expr) => {
        match $body {
            Ok(r) => r,
            Err(_) => {
                continue;
            }
        }
    };
}

#[macro_export]
macro_rules! some_or_break {
    ($body:expr) => {
        match $body {
            Some(r) => r,
            None => {
                break;
            }
        }
    };
}

#[macro_export]
macro_rules! ok_or_break {
    ($body:expr) => {
        match $body {
            Ok(r) => r,
            Err(_) => {
                break;
            }
        }
    };
}

#[repr(align(64))]
pub struct CacheLineAligned<T>(pub T);

impl<T> From<T> for CacheLineAligned<T> {
    fn from(t: T) -> Self {
        CacheLineAligned(t)
    }
}

impl<T> Deref for CacheLineAligned<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
