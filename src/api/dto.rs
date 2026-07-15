use crate::constants::{
    DOCUMENT_LEN, MAX_CLIENT_NAME_LEN, MAX_DECIMAL_PLACES, MAX_TRANSACTION_AMOUNT, MIN_BIRTH_YEAR,
    MIN_CLIENT_AGE_YEARS,
};
use crate::error::AppError;
use crate::model::{Country, Document};
use chrono::{Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDetails {
    pub client_name: String,
    pub birth_date: NaiveDate,
    pub document_number: Document,
    pub country: Country,
}

impl ClientDetails {
    /// Rejects malformed client data before it is ever persisted.
    pub fn validate(&self) -> Result<(), AppError> {
        validate_client_name(&self.client_name)?;
        validate_document_number(&self.document_number)?;
        validate_birth_date(self.birth_date)?;
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewCreditTransaction {
    pub client_id: Uuid,
    pub credit_amount: Decimal,
}

impl NewCreditTransaction {
    pub fn validate(&self) -> Result<(), AppError> {
        validate_amount(self.credit_amount)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewDebitTransaction {
    pub client_id: Uuid,
    pub debit_amount: Decimal,
}

impl NewDebitTransaction {
    pub fn validate(&self) -> Result<(), AppError> {
        validate_amount(self.debit_amount)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetBalanceRequest {
    pub client_id: Uuid,
}

#[derive(Serialize)]
pub struct GetBalanceResponse {
    pub client_id: Uuid,
    pub details: ClientDetails,
    pub balance: Decimal,
}

/// Validates a client's name: non-blank, within the length cap, and free of
/// control characters.
fn validate_client_name(name: &str) -> Result<(), AppError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation("client_name must not be blank".into()));
    }
    if trimmed.chars().count() > MAX_CLIENT_NAME_LEN {
        return Err(AppError::Validation(format!(
            "client_name must be at most {MAX_CLIENT_NAME_LEN} characters"
        )));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(AppError::Validation(
            "client_name must not contain control characters".into(),
        ));
    }
    Ok(())
}

/// Validates a document number's format: digits only within the allowed length.
fn validate_document_number(document: &Document) -> Result<(), AppError> {
    let len = document.len();
    if len != DOCUMENT_LEN {
        return Err(AppError::Validation(format!(
            "document_number must be {DOCUMENT_LEN} digits"
        )));
    }
    if !document.chars().all(|c| c.is_ascii_digit()) {
        return Err(AppError::Validation(
            "document_number must contain digits only".into(),
        ));
    }
    Ok(())
}

/// Validates a birth date: not in the future, not implausibly old, and old
/// enough to register.
fn validate_birth_date(birth_date: NaiveDate) -> Result<(), AppError> {
    let today = Utc::now().date_naive();

    if birth_date > today {
        return Err(AppError::Validation(
            "birth_date must not be in the future".into(),
        ));
    }
    if birth_date.year() < MIN_BIRTH_YEAR {
        return Err(AppError::Validation(format!(
            "birth_date must not be before {MIN_BIRTH_YEAR}"
        )));
    }

    // Age in whole years: subtract one if this year's birthday hasn't happened yet.
    let mut age = today.year() - birth_date.year();
    if (today.month(), today.day()) < (birth_date.month(), birth_date.day()) {
        age -= 1;
    }
    if age < MIN_CLIENT_AGE_YEARS {
        return Err(AppError::Validation(format!(
            "client must be at least {MIN_CLIENT_AGE_YEARS} years old"
        )));
    }
    Ok(())
}

/// Validates a transaction amount: strictly positive, no more than the allowed
/// number of decimal places, and within the per-transaction cap.
fn validate_amount(amount: Decimal) -> Result<(), AppError> {
    if amount <= Decimal::ZERO {
        return Err(AppError::Validation("Amount must be positive".into()));
    }
    if amount.scale() > MAX_DECIMAL_PLACES {
        return Err(AppError::Validation(format!(
            "Amount must have at most {MAX_DECIMAL_PLACES} decimal places"
        )));
    }
    if amount > Decimal::from(MAX_TRANSACTION_AMOUNT) {
        return Err(AppError::Validation(format!(
            "Amount must not exceed {MAX_TRANSACTION_AMOUNT}"
        )));
    }
    Ok(())
}
