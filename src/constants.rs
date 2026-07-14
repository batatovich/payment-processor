/// Root data storage directory location
pub const DATA_DIR: &str = "data";

/// Global metadata ledger containing all registered clients
pub const FILE_CLIENTS_METADATA: &str = "clients.dat";

/// Prefix of the checkpoint header line stored as the first line of the clients
/// metadata file. It records the highest ledger nonce whose deltas have already
/// been folded into the persisted balances, so bootstrap only replays newer ones.
pub const CHECKPOINT_HEADER_PREFIX: &str = "#checkpoint_nonce=";
