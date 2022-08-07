use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy)]
pub enum Progress {
	Range { cur: u64, max: u64 },
	Ratio(f64),
	Count(u64),
	Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Level {
	Debug = 0,
	Info = 1,
	Warning = 2,
	Error = 3,
}

impl Level {
	fn _cmp(&self, other: &Self) -> Ordering {
		if self == other {
			return Ordering::Equal;
		}
		match self {
			Self::Debug => Ordering::Less,
			Self::Info => match other {
				Self::Debug => Ordering::Greater,
				_ => Ordering::Less,
			},
			Self::Warning => match other {
				Self::Error => Ordering::Less,
				_ => Ordering::Greater,
			},
			Self::Error => Ordering::Greater,
		}
	}
}

impl fmt::Display for Level {
	fn fmt<'f>(&self, f: &'f mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Debug => f.write_str("debug"),
			Self::Info => f.write_str("info"),
			Self::Warning => f.write_str("warning"),
			Self::Error => f.write_str("error"),
		}
	}
}

impl PartialOrd for Level {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self._cmp(other))
	}
}

impl Ord for Level {
	fn cmp(&self, other: &Self) -> Ordering {
		self._cmp(other)
	}
}

impl From<Level> for log::Level {
	fn from(other: Level) -> Self {
		match other {
			Level::Debug => log::Level::Debug,
			Level::Info => log::Level::Info,
			Level::Warning => log::Level::Warn,
			Level::Error => log::Level::Error,
		}
	}
}

pub trait DiagnosticsSink {
	fn progress(&mut self, progress: Progress);
	fn log(&mut self, level: Level, subsystem: &str, message: &str);
}

impl<P: DiagnosticsSink> DiagnosticsSink for &mut P {
	fn progress(&mut self, progress: Progress) {
		(**self).progress(progress)
	}

	fn log(&mut self, level: Level, subsystem: &str, message: &str) {
		(**self).log(level, subsystem, message)
	}
}

impl<P: DiagnosticsSink> DiagnosticsSink for Box<P> {
	fn progress(&mut self, progress: Progress) {
		(**self).progress(progress)
	}

	fn log(&mut self, level: Level, subsystem: &str, message: &str) {
		(**self).log(level, subsystem, message)
	}
}

impl DiagnosticsSink for Box<dyn DiagnosticsSink> {
	fn progress(&mut self, progress: Progress) {
		(**self).progress(progress)
	}

	fn log(&mut self, level: Level, subsystem: &str, message: &str) {
		(**self).log(level, subsystem, message)
	}
}

impl<'x> DiagnosticsSink for &mut (dyn DiagnosticsSink + Send + 'x) {
	fn progress(&mut self, progress: Progress) {
		(**self).progress(progress)
	}

	fn log(&mut self, level: Level, subsystem: &str, message: &str) {
		(**self).log(level, subsystem, message)
	}
}

impl<'x> DiagnosticsSink for Box<(dyn DiagnosticsSink + Send + 'x)> {
	fn progress(&mut self, progress: Progress) {
		(**self).progress(progress)
	}

	fn log(&mut self, level: Level, subsystem: &str, message: &str) {
		(**self).log(level, subsystem, message)
	}
}

impl<P: DiagnosticsSink> DiagnosticsSink for Option<P> {
	fn progress(&mut self, progress: Progress) {
		match self.as_mut() {
			Some(v) => v.progress(progress),
			None => (),
		}
	}

	fn log(&mut self, level: Level, subsystem: &str, message: &str) {
		match self.as_mut() {
			Some(v) => v.log(level, subsystem, message),
			None => (),
		}
	}
}
