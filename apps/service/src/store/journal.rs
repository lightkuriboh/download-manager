use download_engine::Checkpoint;
use download_platform::staging::IDENTITY_BYTES;
use rusqlite::{OptionalExtension, params};

use super::{Error, Queue, Result, sql_size};

pub struct Journal {
    pub nonce: String,
    pub identity: [u8; IDENTITY_BYTES],
    pub checkpoint: Option<Checkpoint>,
    pub prepared: Option<u64>,
}

pub type RetiredStage = (String, String, [u8; IDENTITY_BYTES]);

impl Queue {
    pub fn retired_stages(
        &mut self,
        job_id: Option<i64>,
        after: &str,
    ) -> Result<Vec<RetiredStage>> {
        let mut query = self.0.prepare(
            "SELECT nonce, output, identity FROM retired_stages
             WHERE nonce > ?1 AND (?2 IS NULL OR job_id = ?2)
             ORDER BY nonce LIMIT 32",
        )?;
        let mut rows = query.query(params![after, job_id])?;
        let mut stages = Vec::new();
        while let Some(row) = rows.next()? {
            let identity = row
                .get::<_, Vec<u8>>(2)?
                .try_into()
                .map_err(|_| Error::Unavailable)?;
            stages.push((row.get(0)?, row.get(1)?, identity));
        }
        Ok(stages)
    }

    pub fn clear_retired_stage(&mut self, nonce: &str) -> Result<()> {
        self.0
            .execute("DELETE FROM retired_stages WHERE nonce = ?1", [nonce])?;
        Ok(())
    }

    pub fn journal(&mut self, job_id: i64) -> Result<Option<Journal>> {
        let row = self
            .0
            .query_row(
                "SELECT nonce, identity, durable_bytes, total_bytes, etag, prepared_bytes
                 FROM transfer_journal WHERE job_id = ?1",
                [job_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, Option<Vec<u8>>>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(nonce, identity, offset, total, etag, prepared)| {
            let identity = identity.try_into().map_err(|_| Error::Unavailable)?;
            let checkpoint = match (offset, total, etag) {
                (0, None, None) => None,
                (offset, Some(total), Some(etag)) if offset > 0 && offset < total => {
                    Some(Checkpoint {
                        offset: unsigned_size(offset)?,
                        total: unsigned_size(total)?,
                        etag,
                    })
                }
                _ => return Err(Error::Unavailable),
            };
            Ok(Journal {
                nonce,
                identity,
                checkpoint,
                prepared: prepared.map(unsigned_size).transpose()?,
            })
        })
        .transpose()
    }

    pub fn record_stage(&mut self, job_id: i64, nonce: &str, identity: &[u8]) -> Result<()> {
        let inserted = self.0.execute(
            "INSERT INTO transfer_journal (job_id, nonce, identity)
             SELECT id, ?2, ?3 FROM jobs WHERE id = ?1 AND state = 'running'",
            params![job_id, nonce, identity],
        )?;
        self.require_update(inserted)
    }

    pub fn checkpoint(&mut self, job_id: i64, checkpoint: Option<Checkpoint>) -> Result<()> {
        let (durable, total, etag) = match checkpoint {
            Some(Checkpoint {
                offset,
                total,
                etag,
            }) => (sql_size(offset)?, Some(sql_size(total)?), Some(etag)),
            None => (0, None, None),
        };
        let changed = self.0.execute(
            "UPDATE transfer_journal SET durable_bytes = ?2, total_bytes = ?3, etag = ?4
             WHERE job_id = ?1 AND prepared_bytes IS NULL
             AND (?2 = 0 OR (durable_bytes <= ?2 AND (total_bytes IS NULL OR (total_bytes = ?3 AND etag = ?4))))
             AND EXISTS (SELECT 1 FROM jobs WHERE id = ?1 AND state = 'running')",
            params![job_id, durable, total, etag],
        )?;
        self.require_update(changed)
    }

    pub fn prepare_publication(&mut self, job_id: i64, bytes: u64) -> Result<bool> {
        let changed = self.0.execute(
            "UPDATE transfer_journal SET prepared_bytes = ?2
             WHERE job_id = ?1 AND prepared_bytes IS NULL AND (total_bytes IS NULL OR total_bytes = ?2)
             AND EXISTS (SELECT 1 FROM jobs WHERE id = ?1 AND state = 'running' AND stop_intent IS NULL)",
            params![job_id, sql_size(bytes)?],
        )?;
        if changed == 0 && self.should_stop(job_id)? {
            return Ok(false);
        }
        self.require_update(changed)?;
        Ok(true)
    }

    pub fn confirm_publication(&mut self, job_id: i64, bytes: u64) -> Result<()> {
        let changed = self.0.execute(
            "UPDATE jobs SET state = 'completed', bytes = ?2
             WHERE id = ?1 AND state = 'running' AND EXISTS (
                 SELECT 1 FROM transfer_journal WHERE job_id = ?1 AND prepared_bytes = ?2
             )",
            params![job_id, sql_size(bytes)?],
        )?;
        self.require_update(changed)
    }

    pub fn completed_stages(&mut self, after: i64) -> Result<Vec<(i64, String)>> {
        let mut statement = self.0.prepare(
            "SELECT id, output FROM jobs INNER JOIN transfer_journal ON job_id = id
             WHERE state = 'completed' AND id > ?1 ORDER BY id LIMIT 32",
        )?;
        Ok(statement
            .query_map([after], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn clear_stage(&mut self, job_id: i64) -> Result<()> {
        self.0.execute(
            "DELETE FROM transfer_journal WHERE job_id = ?1
             AND EXISTS (SELECT 1 FROM jobs WHERE id = ?1 AND state = 'completed')",
            [job_id],
        )?;
        Ok(())
    }

    fn require_update(&self, changed: usize) -> Result<()> {
        if changed == 1 {
            Ok(())
        } else {
            Err(Error::Unavailable)
        }
    }
}

fn unsigned_size(bytes: i64) -> Result<u64> {
    u64::try_from(bytes).map_err(|_| Error::Unavailable)
}
