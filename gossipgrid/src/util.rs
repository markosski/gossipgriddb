use std::fmt;

pub struct DisplayVec<T>(pub Vec<T>);

impl<T: fmt::Display> fmt::Display for DisplayVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Access the inner vector using tuple indexing (self.0)
        write!(f, "[")?;
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            // Use the Display implementation of the element T
            write!(f, "{}", v)?;
        }
        write!(f, "]")
    }
}
