#[derive(Debug, Clone, Copy)]
pub enum Progress {
	Range { cur: u64, max: u64 },
	Ratio(f64),
	Count(u64),
}

pub trait ProgressSink {
	fn report(&mut self, progress: Progress);
}

impl<P: ProgressSink> ProgressSink for &mut P {
	fn report(&mut self, progress: Progress) {
		(**self).report(progress)
	}
}

impl<P: ProgressSink> ProgressSink for Box<P> {
	fn report(&mut self, progress: Progress) {
		(**self).report(progress)
	}
}

impl ProgressSink for Box<dyn ProgressSink> {
	fn report(&mut self, progress: Progress) {
		(**self).report(progress)
	}
}

impl ProgressSink for &mut dyn ProgressSink {
	fn report(&mut self, progress: Progress) {
		(**self).report(progress)
	}
}

impl<P: ProgressSink> ProgressSink for Option<P> {
	fn report(&mut self, progress: Progress) {
		match self.as_mut() {
			Some(v) => v.report(progress),
			None => (),
		}
	}
}

pub struct FnProgress<F: FnMut(Progress) -> ()>(F);

impl<F: FnMut(Progress) -> ()> From<F> for FnProgress<F> {
	fn from(other: F) -> Self {
		Self(other)
	}
}

impl<F: FnMut(Progress) -> ()> ProgressSink for FnProgress<F> {
	fn report(&mut self, progress: Progress) {
		self.0(progress)
	}
}
