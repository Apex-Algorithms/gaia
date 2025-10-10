use sqlx::{Postgres, Row};
use std::sync::Arc;
use uuid::Uuid;

use crate::{
    error::IndexingError,
    storage::{postgres::PostgresStorage, StorageError},
};

/// Test-specific storage wrapper that provides additional functionality for testing
/// without polluting the production PostgresStorage implementation
pub struct TestStorage {
    storage: Arc<PostgresStorage>,
}

impl TestStorage {
    pub fn new(storage: Arc<PostgresStorage>) -> Self {
        Self { storage }
    }

    /// Get the underlying PostgresStorage for calling production methods
    pub fn storage(&self) -> &PostgresStorage {
        &self.storage
    }

    /// Get direct access to the database pool for custom queries in tests
    pub fn get_pool(&self) -> &sqlx::Pool<Postgres> {
        &self.storage.pool
    }

    /// Test helper: Get space data by DAO addresses
    pub async fn get_spaces_by_dao_addresses(
        &self,
        dao_addresses: &[String],
    ) -> Result<Vec<SpaceRow>, IndexingError> {
        let rows = sqlx::query!(
            "SELECT id, dao_address, type::text as type, space_address, main_voting_address, membership_address, personal_address FROM spaces WHERE dao_address = ANY($1) ORDER BY dao_address",
            dao_addresses
        )
        .fetch_all(self.get_pool())
        .await
        .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(rows
            .into_iter()
            .map(|row| SpaceRow {
                id: row.id,
                dao_address: row.dao_address,
                space_type: row.r#type,
                space_address: row.space_address,
                main_voting_address: row.main_voting_address,
                membership_address: row.membership_address,
                personal_address: row.personal_address,
            })
            .collect())
    }

    /// Test helper: Get entity data by ID
    pub async fn get_entity_by_id(
        &self,
        entity_id: &Uuid,
    ) -> Result<Option<EntityRow>, IndexingError> {
        let row = sqlx::query!(
            "SELECT id, created_at, created_at_block, updated_at, updated_at_block FROM entities WHERE id = $1",
            entity_id
        )
        .fetch_optional(self.get_pool())
        .await
        .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(row.map(|r| EntityRow {
            id: r.id,
            created_at: r.created_at,
            created_at_block: r.created_at_block,
            updated_at: r.updated_at,
            updated_at_block: r.updated_at_block,
        }))
    }

    /// Test helper: Get values for an entity
    pub async fn get_values_by_entity_id(
        &self,
        entity_id: &Uuid,
    ) -> Result<Vec<ValueRow>, IndexingError> {
        let rows = sqlx::query!(
            r#"SELECT
                id, property_id, entity_id, space_id,
                language, unit, string,
                number::text as number,
                boolean, time, point
                FROM values WHERE entity_id = $1"#,
            entity_id
        )
        .fetch_all(self.get_pool())
        .await
        .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(rows
            .into_iter()
            .map(|row| ValueRow {
                id: Uuid::parse_str(&row.id).unwrap(),
                property_id: row.property_id,
                entity_id: row.entity_id,
                space_id: row.space_id,
                language: row.language,
                unit: row.unit,
                string: row.string,
                number: row.number.as_ref().and_then(|n| n.parse::<f64>().ok()),
                boolean: row.boolean,
                time: row.time,
                point: row.point,
            })
            .collect())
    }

    /// Test helper: Get relations by entity ID
    pub async fn get_relations_by_entity_id(
        &self,
        entity_id: &Uuid,
    ) -> Result<Vec<RelationRow>, IndexingError> {
        let rows = sqlx::query!(
            "SELECT id, entity_id, type_id, from_entity_id, from_space_id, from_version_id, to_entity_id, to_space_id, to_version_id, position, space_id, verified FROM relations WHERE entity_id = $1",
            entity_id
        )
        .fetch_all(self.get_pool())
        .await
        .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(rows
            .into_iter()
            .map(|row| RelationRow {
                id: row.id,
                entity_id: row.entity_id,
                type_id: row.type_id,
                from_entity_id: row.from_entity_id,
                from_space_id: row.from_space_id,
                from_version_id: row.from_version_id,
                to_entity_id: row.to_entity_id,
                to_space_id: row.to_space_id,
                to_version_id: row.to_version_id,
                position: row.position,
                space_id: row.space_id,
                verified: row.verified,
            })
            .collect())
    }

    /// Test helper: Count total records in a table
    pub async fn count_records(&self, table_name: &str) -> Result<i64, IndexingError> {
        let query = format!("SELECT COUNT(*) as count FROM {}", table_name);
        let row = sqlx::query(&query)
            .fetch_one(self.get_pool())
            .await
            .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(row.get("count"))
    }

    /// Test helper: Clear all data from a table (use with caution!)
    pub async fn clear_table(&self, table_name: &str) -> Result<(), IndexingError> {
        let query = format!("DELETE FROM {}", table_name);
        sqlx::query(&query)
            .execute(self.get_pool())
            .await
            .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(())
    }

    /// Test helper: Get proposals by space ID
    pub async fn get_proposals_by_space(
        &self,
        space_id: &Uuid,
    ) -> Result<Vec<ProposalRow>, IndexingError> {
        let rows = sqlx::query!(
            r#"SELECT 
                id, space_id, proposal_type::text as proposal_type, creator, 
                start_time, end_time, status::text as status, content_uri, address, created_at_block
                FROM proposals WHERE space_id = $1 ORDER BY created_at_block"#,
            space_id
        )
        .fetch_all(self.get_pool())
        .await
        .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(rows
            .into_iter()
            .map(|row| ProposalRow {
                id: row.id,
                space_id: row.space_id,
                proposal_type: row.proposal_type.unwrap_or_default(),
                creator: row.creator,
                start_time: row.start_time,
                end_time: row.end_time,
                status: row.status.unwrap_or_default(),
                content_uri: row.content_uri,
                address: row.address,
                created_at_block: row.created_at_block,
            })
            .collect())
    }

    /// Test helper: Get proposal by ID
    pub async fn get_proposal_by_id(
        &self,
        proposal_id: &Uuid,
    ) -> Result<Option<ProposalRow>, IndexingError> {
        let row = sqlx::query!(
            r#"SELECT 
                id, space_id, proposal_type::text as proposal_type, creator, 
                start_time, end_time, status::text as status, content_uri, address, created_at_block
                FROM proposals WHERE id = $1"#,
            proposal_id
        )
        .fetch_optional(self.get_pool())
        .await
        .map_err(|e| IndexingError::StorageError(StorageError::Database(e)))?;

        Ok(row.map(|r| ProposalRow {
            id: r.id,
            space_id: r.space_id,
            proposal_type: r.proposal_type.unwrap_or_default(),
            creator: r.creator,
            start_time: r.start_time,
            end_time: r.end_time,
            status: r.status.unwrap_or_default(),
            content_uri: r.content_uri,
            address: r.address,
            created_at_block: r.created_at_block,
        }))
    }
}

/// Test data structures for database row verification
#[derive(Debug, Clone)]
pub struct SpaceRow {
    pub id: Uuid,
    pub dao_address: String,
    pub space_type: Option<String>,
    pub space_address: String,
    pub main_voting_address: Option<String>,
    pub membership_address: Option<String>,
    pub personal_address: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EntityRow {
    pub id: Uuid,
    pub created_at: String,
    pub created_at_block: String,
    pub updated_at: String,
    pub updated_at_block: String,
}

#[derive(Debug, Clone)]
pub struct ValueRow {
    pub id: Uuid,
    pub property_id: Uuid,
    pub entity_id: Uuid,
    pub space_id: Uuid,
    pub language: Option<String>,
    pub unit: Option<String>,
    pub string: Option<String>,
    pub number: Option<f64>,
    pub boolean: Option<bool>,
    pub time: Option<String>,
    pub point: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RelationRow {
    pub id: Uuid,
    pub entity_id: Uuid,
    pub type_id: Uuid,
    pub from_entity_id: Uuid,
    pub from_space_id: Option<Uuid>,
    pub from_version_id: Option<Uuid>,
    pub to_entity_id: Uuid,
    pub to_space_id: Option<Uuid>,
    pub to_version_id: Option<Uuid>,
    pub position: Option<String>,
    pub space_id: Uuid,
    pub verified: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ProposalRow {
    pub id: Uuid,
    pub space_id: Uuid,
    pub proposal_type: String,
    pub creator: String,
    pub start_time: i64,
    pub end_time: i64,
    pub status: String,
    pub content_uri: Option<String>,
    pub address: Option<String>,
    pub created_at_block: i64,
}

impl SpaceRow {
    /// Helper method to check if this is a personal space
    pub fn is_personal(&self) -> bool {
        self.space_type
            .as_ref()
            .map(|t| t == "Personal")
            .unwrap_or(false)
    }

    /// Helper method to check if this is a public space
    pub fn is_public(&self) -> bool {
        self.space_type
            .as_ref()
            .map(|t| t == "Public")
            .unwrap_or(false)
    }

    /// Validate that a personal space has correct field values
    pub fn validate_personal_space(&self) -> Result<(), String> {
        if !self.is_personal() {
            return Err(format!(
                "Expected Personal space, got {:?}",
                self.space_type
            ));
        }
        if self.main_voting_address.is_some() {
            return Err("Personal space should not have main_voting_address".to_string());
        }
        if self.membership_address.is_some() {
            return Err("Personal space should not have membership_address".to_string());
        }
        if self.personal_address.is_none() {
            return Err("Personal space must have personal_address".to_string());
        }
        Ok(())
    }

    /// Validate that a public space has correct field values
    pub fn validate_public_space(&self) -> Result<(), String> {
        if !self.is_public() {
            return Err(format!("Expected Public space, got {:?}", self.space_type));
        }
        if self.main_voting_address.is_none() {
            return Err("Public space must have main_voting_address".to_string());
        }
        if self.membership_address.is_none() {
            return Err("Public space must have membership_address".to_string());
        }
        if self.personal_address.is_some() {
            return Err("Public space should not have personal_address".to_string());
        }
        Ok(())
    }
}
