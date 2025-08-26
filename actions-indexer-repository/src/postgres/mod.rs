//! PostgreSQL implementation of the actions indexer repository.
//!
//! Provides a production-ready PostgreSQL backend for the `ActionsRepository` trait
//! with connection pooling, transaction safety, and batch operations.
//!
//! ## Key Features
//!
//! - Connection pooling with `sqlx::PgPool`
//! - ACID transactions with automatic rollback
//! - Bulk operations using PostgreSQL's `UNNEST` and `VALUES`
//! - Upsert support with `ON CONFLICT DO UPDATE`
//! - Type-safe queries with SQLx
//!
//! ## Database Tables
//!
//! - `raw_actions`: Processed blockchain actions
//! - `user_votes`: Individual voting records with upsert support
//! - `votes_count`: Aggregated vote tallies per entity/space
use async_trait::async_trait;
use actions_indexer_shared::types::{Action, Changeset, UserVote, VotesCount, EntityId, VoteCriteria, VoteCountCriteria, VoteValue};
use crate::{ActionsRepository, ActionsRepositoryError};
use hex;
use time::OffsetDateTime;
use alloy::{primitives::Address, hex::FromHex};

/// PostgreSQL implementation of the actions indexer repository.
///
/// Provides database operations for actions, user votes, and vote counts using
/// PostgreSQL with connection pooling and transaction support.
///
/// ## Features
///
/// - Connection pooling with `sqlx::PgPool`
/// - Automatic transaction wrapping for all operations
/// - Bulk operations using `QueryBuilder` for performance
/// - Upsert operations with conflict resolution
/// - Efficient batch queries using `UNNEST`
pub struct PostgresActionsRepository {
    pool: sqlx::PgPool,
}

impl PostgresActionsRepository {
    /// Creates a new PostgreSQL repository instance.
    ///
    /// # Arguments
    ///
    /// * `pool` - Configured PostgreSQL connection pool with required schema
    ///
    /// # Returns
    ///
    /// * `Ok(PostgresActionsRepository)` - Ready-to-use repository instance
    /// * `Err(ActionsRepositoryError)` - Future validation errors (currently always succeeds)
    pub async fn new(pool: sqlx::PgPool) -> Result<Self, ActionsRepositoryError> {
        Ok(Self { pool })
    }

    /// Inserts actions within an active transaction using bulk operations.
    ///
    /// Uses `QueryBuilder` for efficient multi-row INSERT into `raw_actions` table.
    /// Handles blockchain addresses as hex-encoded strings and timestamps as PostgreSQL timestamps.
    ///
    /// # Arguments
    ///
    /// * `actions` - Actions to insert (empty slices are no-ops)
    /// * `tx` - Active transaction context
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All actions inserted successfully
    /// * `Err(ActionsRepositoryError)` - Database or encoding error
    async fn insert_actions_tx(&self, actions: &[Action], tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), ActionsRepositoryError> {
        if actions.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO raw_actions (action_type, action_version, sender, entity, group_id, space_pov, metadata, block_number, block_timestamp, tx_hash) "
        );

        query_builder.push_values(actions, |mut b, action| {
            match action {
                Action::Vote(vote_action) => {
                    // TODO: extract to a helper function
                    let voted_at = OffsetDateTime::from_unix_timestamp(vote_action.raw.block_timestamp as i64)
                        .unwrap_or(OffsetDateTime::now_utc());
                    b.push_bind(vote_action.raw.action_type as i64)
                     .push_bind(vote_action.raw.action_version as i64)
                     .push_bind(format!("0x{}", hex::encode(vote_action.raw.sender.as_slice())))
                     .push_bind(vote_action.raw.entity.clone())
                     .push_bind(vote_action.raw.group_id.clone())
                     .push_bind(format!("0x{}", hex::encode(vote_action.raw.space_pov.as_slice())))
                     .push_bind(vote_action.raw.metadata.as_ref().map(|b| b.as_ref().to_vec()))
                     .push_bind(vote_action.raw.block_number as i64)
                     .push_bind(voted_at)
                     .push_bind(format!("0x{}", hex::encode(vote_action.raw.tx_hash.as_slice())));
                }
            }
        });

        query_builder.build().execute(&mut **tx).await?;
        Ok(())
    }

    /// Updates user votes within an active transaction using upsert operations.
    ///
    /// Uses `ON CONFLICT DO UPDATE` for each vote record targeting the `user_votes` table
    /// with composite key (user_id, entity_id, space_id). Addresses are hex-encoded.
    ///
    /// # Arguments
    ///
    /// * `user_votes` - Vote records to upsert (empty slices are no-ops)
    /// * `tx` - Active transaction context
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All votes processed successfully
    /// * `Err(ActionsRepositoryError)` - Database or encoding error
    async fn update_user_votes_tx(&self, user_votes: &[UserVote], tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), ActionsRepositoryError> {
        if user_votes.is_empty() {
            return Ok(());
        }

        for vote in user_votes {
            sqlx::query!(
                r#"
                INSERT INTO user_votes (user_id, entity_id, space_id, vote_type, voted_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, entity_id, space_id)
                DO UPDATE SET
                    vote_type = EXCLUDED.vote_type,
                    voted_at = EXCLUDED.voted_at
                "#,
                format!("0x{}", hex::encode(vote.user_id.as_slice())),
                vote.entity_id.clone(),
                format!("0x{}", hex::encode(vote.space_id.as_slice())),
                match vote.vote_type {
                    VoteValue::Up => 0,
                    VoteValue::Down => 1,
                    VoteValue::Remove => 2,
                } as i16,
                OffsetDateTime::from_unix_timestamp(vote.voted_at as i64)
                    .unwrap_or(OffsetDateTime::now_utc())
            )
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }

    /// Updates vote count aggregations within an active transaction.
    ///
    /// Uses upsert operations on `votes_count` table with composite key (entity_id, space_id).
    /// Replaces existing totals with new values to maintain accurate statistics.
    ///
    /// # Arguments
    ///
    /// * `votes_counts` - Count records to upsert (empty slices are no-ops)
    /// * `tx` - Active transaction context
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All counts updated successfully
    /// * `Err(ActionsRepositoryError)` - Database or encoding error
    async fn update_votes_counts_tx(&self, votes_counts: &[VotesCount], tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), ActionsRepositoryError> {
        if votes_counts.is_empty() {
            return Ok(());
        }

        for count in votes_counts { 
            sqlx::query!(
                r#"
                INSERT INTO votes_count (entity_id, space_id, upvotes, downvotes)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (entity_id, space_id)
                DO UPDATE SET 
                    upvotes = EXCLUDED.upvotes,
                    downvotes = EXCLUDED.downvotes
                "#,
                count.entity_id.clone(),
                format!("0x{}", hex::encode(count.space_id.as_slice())),
                count.upvotes,
                count.downvotes
            )
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ActionsRepository for PostgresActionsRepository {
    /// Inserts actions into the repository using a new transaction.
    ///
    /// Creates a transaction, performs bulk insertion, and commits atomically.
    /// Empty slices are handled efficiently as no-ops.
    ///
    /// # Arguments
    ///
    /// * `actions` - Actions to insert
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All actions inserted successfully
    /// * `Err(ActionsRepositoryError)` - Transaction or insertion failure
    async fn insert_actions(
        &self,
        actions: &[Action],
    ) -> Result<(), ActionsRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        self.insert_actions_tx(actions, &mut tx).await?;
        tx.commit().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        Ok(())
    }

    /// Updates user votes using upsert operations in a new transaction.
    ///
    /// Handles conflicts by updating existing votes with new data.
    /// Empty slices are handled efficiently as no-ops.
    ///
    /// # Arguments
    ///
    /// * `user_votes` - User votes to update/insert
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All votes updated successfully
    /// * `Err(ActionsRepositoryError)` - Transaction or update failure
    async fn update_user_votes(
        &self,
        user_votes: &[UserVote],
    ) -> Result<(), ActionsRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        self.update_user_votes_tx(user_votes, &mut tx).await?;
        tx.commit().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        Ok(())
    }

    /// Updates aggregated vote counts in a new transaction.
    ///
    /// Replaces existing count totals for each entity-space combination.
    /// Empty slices are handled efficiently as no-ops.
    ///
    /// # Arguments
    ///
    /// * `votes_counts` - Vote count records to update
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All counts updated successfully
    /// * `Err(ActionsRepositoryError)` - Transaction or update failure
    async fn update_votes_counts(
        &self,
        votes_counts: &[VotesCount],
    ) -> Result<(), ActionsRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        self.update_votes_counts_tx(votes_counts, &mut tx).await?;
        tx.commit().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        Ok(())
    }

    /// Atomically persists a complete changeset in a single transaction.
    ///
    /// Bundles actions, user votes, and vote counts together for atomic persistence.
    /// Either all changes succeed or all are rolled back on failure.
    ///
    /// # Arguments
    ///
    /// * `changeset` - Changeset containing related data modifications
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Entire changeset persisted successfully
    /// * `Err(ActionsRepositoryError)` - Transaction failure with automatic rollback
    async fn persist_changeset(
        &self,
        changeset: &Changeset<'_>,
    ) -> Result<(), ActionsRepositoryError> {
        let mut tx = self.pool.begin().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        self.insert_actions_tx(changeset.actions, &mut tx).await?;
        self.update_user_votes_tx(changeset.user_votes, &mut tx).await?;
        self.update_votes_counts_tx(changeset.votes_count, &mut tx).await?;
        tx.commit().await.map_err(|e| ActionsRepositoryError::DatabaseError(e))?;
        Ok(())
    }

    /// Retrieves user votes matching the specified criteria.
    ///
    /// Uses PostgreSQL's UNNEST function for efficient batch queries of multiple
    /// user-entity-space combinations in a single database operation.
    ///
    /// # Arguments
    ///
    /// * `vote_criteria` - Tuples of (user_id, entity_id, space_id) to query
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<UserVote>)` - Matching votes (empty if none found)
    /// * `Err(ActionsRepositoryError)` - Database query failure
    async fn get_user_votes(&self, vote_criteria: &[VoteCriteria]) -> Result<Vec<UserVote>, ActionsRepositoryError> {
        if vote_criteria.is_empty() {
            return Ok(Vec::new());
        }

        let user_ids: Vec<String> = vote_criteria.iter().map(|(u, _, _)| format!("0x{}", hex::encode(u.as_slice()))).collect();
        let entity_ids: Vec<EntityId> = vote_criteria.iter().map(|(_, e, _)| *e).collect();
        let space_ids: Vec<String> = vote_criteria.iter().map(|(_, _, s)| format!("0x{}", hex::encode(s.as_slice()))).collect();

        let votes = sqlx::query!(
            r#"
            SELECT user_id, entity_id, space_id, vote_type, voted_at
            FROM user_votes
            WHERE (user_id, entity_id, space_id) IN (SELECT * FROM UNNEST($1::text[], $2::uuid[], $3::text[]))
            "#,
            &user_ids,
            &entity_ids,
            &space_ids,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut result_votes = Vec::with_capacity(votes.len());
        for v in votes {
            result_votes.push(UserVote {
                user_id: Address::from_hex(&v.user_id).map_err(|_| ActionsRepositoryError::InvalidAddress(v.user_id))?,
                entity_id: v.entity_id,
                space_id: Address::from_hex(&v.space_id).map_err(|_| ActionsRepositoryError::InvalidAddress(v.space_id))?,
                vote_type: match v.vote_type {
                    0 => VoteValue::Up,
                    1 => VoteValue::Down,
                    2 => VoteValue::Remove,
                    _ => return Err(ActionsRepositoryError::InvalidVoteType(v.vote_type)),
                },
                voted_at: v.voted_at.unix_timestamp() as u64,
            });
        }

        Ok(result_votes)
    }

    /// Retrieves aggregated vote counts for entities and spaces.
    ///
    /// Efficiently queries vote statistics using PostgreSQL's UNNEST function for
    /// batch lookups of entity-space combinations.
    ///
    /// # Arguments
    ///
    /// * `vote_criteria` - Tuples of (entity_id, space_id) to query
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<VotesCount>)` - Matching vote counts (empty if none found)
    /// * `Err(ActionsRepositoryError)` - Database query failure
    async fn get_vote_counts(&self, vote_criteria: &[VoteCountCriteria]) -> Result<Vec<VotesCount>, ActionsRepositoryError> {
        if vote_criteria.is_empty() {
            return Ok(Vec::new());
        }

        let entity_ids: Vec<EntityId> = vote_criteria.iter().map(|(e, _)| *e).collect();
        let space_ids: Vec<String> = vote_criteria.iter().map(|(_, s)| format!("0x{}", hex::encode(s.as_slice()))).collect();

        let counts = sqlx::query!(
            r#"
            SELECT entity_id, space_id, upvotes, downvotes
            FROM votes_count
            WHERE (entity_id, space_id) IN (SELECT * FROM UNNEST($1::uuid[], $2::text[]))
            "#,
            &entity_ids,
            &space_ids,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut result_counts = Vec::with_capacity(counts.len());
        for c in counts {
            result_counts.push(VotesCount {
                entity_id: c.entity_id,
                space_id: Address::from_hex(&c.space_id).map_err(|_| ActionsRepositoryError::InvalidAddress(c.space_id))?,
                upvotes: c.upvotes,
                downvotes: c.downvotes,
            });
        }

        Ok(result_counts)
    }
}