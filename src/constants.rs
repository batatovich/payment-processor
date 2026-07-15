/// Root data storage directory location
pub const DATA_DIR: &str = "data";

/// Global metatada ledger containing all registered clients
pub const FILE_CLIENTS_METADATA: &str = "clients.dat";

/// Address the HTTP server binds to.
pub const SERVER_HOST: &str = "127.0.0.1";
pub const SERVER_PORT: u16 = 8080;

// --- Request body validation limits ---

/// Maximum number of caracters allowed in a client's name
pub const MAX_CLIENT_NAME_LEN: usize = 128;

/// Number of characters in a document number.
pub const DOCUMENT_LEN: usize = 8;

/// Earliest plausible birth year we accept  for a client
pub const MIN_BIRTH_YEAR: i32 = 1900;

/// Minimum age (in years) a client must be to register.
pub const MIN_CLIENT_AGE_YEARS: i32 = 18;

/// Maximum number of decimal places allowed on a transaction amount.
pub const MAX_DECIMAL_PLACES: u32 = 3;

/// Maximum absolute value accepted for a single transaction ammount.
pub const MAX_TRANSACTION_AMOUNT: i64 = 1_000_000;
