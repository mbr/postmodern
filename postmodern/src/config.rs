//! Configuration loading for postmodern.
//!
//! Loads database connection settings from a TOML config file at
//! `$XDG_CONFIG_HOME/postmodern/config.toml` (typically `~/.config/postmodern/config.toml`).

use std::{fs, path::PathBuf};

use serde::Deserialize;
use thiserror::Error;

/// Configuration file structure.
#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// Database connection string.
    pub database_url: String,
}

/// Errors that can occur when loading configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// Failed to read the config file.
    #[error("failed to read config file: {0}")]
    Read(#[source] std::io::Error),
    /// Failed to parse the config file.
    #[error("failed to parse config: {0}")]
    Parse(#[source] toml::de::Error),
    /// No database URL available.
    #[error("no database URL provided; use --db or create {}", config_path().map(|p| p.display().to_string()).unwrap_or_else(|| "config file".to_string()))]
    NoDatabaseUrl,
}

/// Returns the path to the config file.
pub fn config_path() -> Option<PathBuf> {
    dirs::config_dir().map(|p| p.join("postmodern").join("config.toml"))
}

/// Loads configuration from the config file.
///
/// Returns `Ok(None)` if no config file exists.
pub fn load_config() -> Result<Option<Config>, ConfigError> {
    let Some(path) = config_path() else {
        return Ok(None);
    };

    if !path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(&path).map_err(ConfigError::Read)?;
    let config: Config = toml::from_str(&contents).map_err(ConfigError::Parse)?;
    Ok(Some(config))
}

/// Resolves the database URL from an override or the config file.
///
/// If `override_url` is `Some`, returns it directly. Otherwise attempts to load
/// from the config file.
pub fn resolve_database_url(override_url: Option<String>) -> Result<String, ConfigError> {
    if let Some(db) = override_url {
        return Ok(db);
    }

    if let Some(config) = load_config()? {
        return Ok(config.database_url);
    }

    Err(ConfigError::NoDatabaseUrl)
}
