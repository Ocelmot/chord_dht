use std::{error::Error, fmt::Display};

use tokio::time::error::Elapsed;

/// Result for [ChordError]
pub type ChordResult<T = ()> = Result<T, ChordError>;

/// Trait allows its implementors to be informed of a problem of type P.
pub trait Problem<T, P> {
    /// Inform the error or result of the type of problem
    fn problem(self, kind: P) -> ChordResult<T>;
}

impl<T, I: Into<ChordError>> Problem<T, ErrorKind> for Result<T, I> {
    fn problem(self, kind: ErrorKind) -> ChordResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => {
                let err = err.into();
                Err(err.problem(kind))
            }
        }
    }
}

/// Wraps an error with [ChordResult], allowing unknown errors to be used as
/// the source for [ChordError] and [ChordResult]
pub trait ProblemWrap<T, P> {
    /// Wraps an error with the [ChordResult].
    /// This does not require that the wrapped error can be converted
    /// into a ChordError.
    fn problem_wrap(self, kind: P) -> ChordResult<T>;

    /// Wraps an error with the [ChordResult].
    fn wrap(self) -> ChordResult<T>;
}

impl<T, E: Error + Send + Sync + 'static> ProblemWrap<T, ErrorKind> for Result<T, E> {
    fn problem_wrap(self, kind: ErrorKind) -> ChordResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(ChordError {
                kind: Some(kind),
                source: Some(Box::new(err)),
            }),
        }
    }

    fn wrap(self) -> ChordResult<T> {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(ChordError {
                kind: None,
                source: Some(Box::new(err)),
            }),
        }
    }
}

/// The category of error the chord encountered
#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    // Internal errors
    /// The chord failed to send some message
    SendError,

    /// Timed out
    Timeout,

    /// The chord was not able to save its state
    SaveFailure,

    // Member errors
    /// Failed to connect to a chord node
    FailedToConnect,

    /// The listener handler for the address and id type failed to start
    ListenerHandlerFailed,

    /// The chord has stopped and cannot process more messages.
    ChordStopped,

    // Associate errors
    /// Associate connection creation failed
    AssociateCreationFailed,

    /// The associate connection failed to connect
    AssociateConnectionFailed,

    /// The associate connection to the chord has closed.
    /// No further messages can be sent.
    AssociateClosed,

    /// The associate connection recieved a response that did not match the
    /// request.
    MismatchedResponse,
}

/// Represents an error that occurred within the chord
#[derive(Debug)]
pub struct ChordError {
    kind: Option<ErrorKind>,
    source: Option<Box<(dyn Error + Send + Sync + 'static)>>,
}

impl ChordError {
    /// Set the kind of error if it is not already set. Otherwise
    /// wrap this error in another error with that [ErrorKind].
    pub fn problem(mut self, kind: ErrorKind) -> Self {
        match &self.kind {
            Some(self_kind) => {
                if *self_kind == kind {
                    // No reason to wrap the error if the kinds already match
                    return self;
                }
                // Wrap self in new error
                ChordError {
                    kind: Some(kind),
                    source: Some(Box::new(self)),
                }
            }
            None => {
                // Set kind
                self.kind = Some(kind);
                self
            }
        }
    }
}

impl Display for ChordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChordError: ")?;
        match &self.kind {
            Some(kind) => match kind {
                ErrorKind::SendError => write!(f, "SendError"),
                ErrorKind::Timeout => write!(f, "Timeout"),
                ErrorKind::SaveFailure => write!(f, "SaveFailure, could not save chord state to the disk"),
                ErrorKind::FailedToConnect => write!(f, "FailedToConnect, could not join chord, could not connect to a node"),
                ErrorKind::ListenerHandlerFailed => write!(f, "ListenerHandlerFailed, the listen handler for this address and id type failed to start"),
                ErrorKind::ChordStopped => write!(f, "ChordStopped, the chord has stopped and will no longer process messages"),
                ErrorKind::AssociateCreationFailed => write!(f, "AssociateCreationFailed, failed to create an associate connection"),
                ErrorKind::AssociateConnectionFailed => write!(f, "AssociateConnectionFailed, failed to connect to the node as an associate"),
                ErrorKind::AssociateClosed => write!(f, "AssociateClosed, the associate connection has closed"),
                ErrorKind::MismatchedResponse => write!(f, "MismatchedResponse, the response is not of the type expected by the request"),
            },
            None => write!(f, "Generic"),
        }
    }
}

impl Error for ChordError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl From<ErrorKind> for ChordError {
    fn from(value: ErrorKind) -> Self {
        Self {
            kind: Some(value),
            source: None,
        }
    }
}

// Error conversion implementations

impl From<std::io::Error> for ChordError {
    fn from(value: std::io::Error) -> Self {
        ChordError {
            kind: None,
            source: Some(Box::new(value)),
        }
    }
}

impl<T: std::fmt::Debug + Send + Sync + 'static> From<tokio::sync::mpsc::error::SendError<T>>
    for ChordError
{
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ChordError {
            kind: Some(ErrorKind::SendError),
            source: Some(Box::new(value)),
        }
    }
}

impl From<Elapsed> for ChordError {
    fn from(value: Elapsed) -> Self {
        ChordError {
            kind: Some(ErrorKind::Timeout),
            source: Some(Box::new(value)),
        }
    }
}
