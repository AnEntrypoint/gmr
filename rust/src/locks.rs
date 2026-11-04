use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::{OwnedMutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub struct Locks {
    shell: RwLock<()>,
    paths: Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>,
}

impl Locks {
    pub fn new() -> Self {
        Locks {
            shell: RwLock::new(()),
            paths: Mutex::new(HashMap::new()),
        }
    }

    pub async fn shared(&self) -> RwLockReadGuard<'_, ()> {
        self.shell.read().await
    }

    pub async fn exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.shell.write().await
    }

    pub async fn path(&self, p: &Path) -> OwnedMutexGuard<()> {
        let key = canonical_key(p);
        let slot = {
            let mut map = self.paths.lock().unwrap_or_else(PoisonError::into_inner);
            map.retain(|_, slot| Arc::strong_count(slot) > 1);
            map.entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        slot.lock_owned().await
    }
}

impl Default for Locks {
    fn default() -> Self {
        Self::new()
    }
}

fn canonical_key(p: &Path) -> PathBuf {
    if let Ok(full) = std::fs::canonicalize(p) {
        return full;
    }
    match (p.parent(), p.file_name()) {
        (Some(parent), Some(name)) => match std::fs::canonicalize(parent) {
            Ok(dir) => dir.join(name),
            Err(_) => p.to_path_buf(),
        },
        _ => p.to_path_buf(),
    }
}
