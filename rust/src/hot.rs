use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    pub len: u64,
    pub modified: Option<SystemTime>,
}

pub fn of(path: &Path) -> Option<Fingerprint> {
    let meta = std::fs::metadata(path).ok()?;
    Some(Fingerprint { len: meta.len(), modified: meta.modified().ok() })
}

pub struct Watch {
    path: PathBuf,
    base: Option<Fingerprint>,
    candidate: Option<Fingerprint>,
}

impl Watch {
    pub fn new(path: PathBuf) -> Watch {
        let base = of(&path);
        Watch { path, base, candidate: None }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn poll(&mut self) -> bool {
        let current = of(&self.path);
        if current.is_none() || current == self.base {
            self.candidate = None;
            return false;
        }
        if current == self.candidate {
            self.base = current;
            self.candidate = None;
            return true;
        }
        self.candidate = current;
        false
    }
}
