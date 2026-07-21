//! Projection apply and read functions. Phase A stubs: signatures are final, bodies
//! return [`Error::Unimplemented`] until the projections subsystem lands.

use rusqlite::{Connection, Transaction};

use crate::error::Error;
use crate::projection::{ProjectionEntry, ProjectionPatch};

/// Apply one validated projection patch inside the commit transaction.
pub(crate) fn apply_projection_patch(
    _tx: &Transaction<'_>,
    _patch: &ProjectionPatch,
) -> Result<(), Error> {
    Err(Error::Unimplemented("projections: apply_projection_patch"))
}

/// The current version of a projection (0 when it does not exist).
pub(crate) fn projection_version(_conn: &Connection, _projection: &str) -> Result<u64, Error> {
    Err(Error::Unimplemented("projections: projection_version"))
}

/// Get one projection entry by key.
pub(crate) fn projection_get(
    _conn: &Connection,
    _projection: &str,
    _key: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    Err(Error::Unimplemented("projections: projection_get"))
}

/// Scan entries with keys starting with `prefix`, in key order.
pub(crate) fn projection_scan_prefix(
    _conn: &Connection,
    _projection: &str,
    _prefix: &[u8],
    _limit: usize,
) -> Result<Vec<ProjectionEntry>, Error> {
    Err(Error::Unimplemented("projections: projection_scan_prefix"))
}

/// List all projections and their versions.
pub(crate) fn projections_list(_conn: &Connection) -> Result<Vec<(String, u64)>, Error> {
    Err(Error::Unimplemented("projections: projections_list"))
}
