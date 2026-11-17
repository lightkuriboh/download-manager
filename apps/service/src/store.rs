#[cfg(unix)]
use download_protocol::{JobAction, JobTransition};
use std::{path::Path, time::Duration};

use download_protocol::{Add, Added, JobId, JobPage, JobSummary, Phase, Retried, Retry, Uuid};
use ring::digest::SHA256;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

const MAX_JOBS: i64 = 100_000;
const PAGE_ROWS: usize = 100;
const PAGE_BYTES: usize = 192 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("only failed or interrupted jobs can be retried")]
    InvalidState,
    #[error("idempotency key was previously used for a different command")]
    KeyConflict,
    #[error("the download queue is full")]
    QueueFull,
    #[error("queue storage failed: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("queue serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("queue storage is unavailable")]
    Unavailable,
}

type Result<T> = std::result::Result<T, Error>;
type Operation = Box<dyn FnOnce(&mut Queue) + Send>;

#[derive(Clone)]
pub struct Store(mpsc::Sender<Operation>);

impl Store {
    pub async fn open(path: &Path) -> Result<(Self, std::thread::JoinHandle<()>)> {
        let (jobs, mut incoming) = mpsc::channel::<Operation>(128);
        let (ready, opened) = oneshot::channel();
        let path = path.to_owned();
        let thread = std::thread::Builder::new()
            .name("download-storage".into())
            .spawn(move || match Queue::open(&path) {
                Ok(mut queue) => {
                    if ready.send(Ok(())).is_ok() {
                        while let Some(run) = incoming.blocking_recv() {
                            run(&mut queue);
                        }
                    }
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
            })
            .map_err(|_| Error::Unavailable)?;
        opened.await.map_err(|_| Error::Unavailable)??;
        Ok((Self(jobs), thread))
    }

    pub async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Queue) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (sender, reply) = oneshot::channel();
        self.0
            .send(Box::new(move |queue| {
                let _ = sender.send(operation(queue));
            }))
            .await
            .map_err(|_| Error::Unavailable)?;
        reply.await.map_err(|_| Error::Unavailable)?
    }
}

pub struct Queue(Connection);

pub struct Job {
    pub id: i64,
    pub url: String,
    pub output: String,
    pub digest: Option<download_engine::Sha256>,
}

impl Queue {
    fn open(path: &Path) -> Result<Self> {
        let mut db = Connection::open(path)?;
        db.busy_timeout(Duration::from_secs(3))?;
        db.execute_batch(
            "PRAGMA foreign_keys = ON;
             PRAGMA journal_mode = WAL;
             PRAGMA synchronous = FULL;",
        )?;
        let migration = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let version: u32 = migration.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        match version {
            0 => migration.execute_batch(
                "CREATE TABLE IF NOT EXISTS jobs (
                     id INTEGER PRIMARY KEY,
                     url TEXT NOT NULL,
                     output TEXT NOT NULL,
                     state TEXT NOT NULL CHECK (
                         state IN ('queued', 'running', 'completed', 'failed', 'interrupted')
                     ),
                     bytes INTEGER CHECK (bytes >= 0)
                 );
                 CREATE INDEX jobs_by_state ON jobs(state, id);
                 CREATE TABLE command_receipts (
                     key TEXT PRIMARY KEY,
                     fingerprint BLOB NOT NULL,
                     result_json TEXT NOT NULL,
                     expires_at INTEGER NOT NULL
                 );
                 CREATE INDEX receipts_by_expiry ON command_receipts(expires_at);
                 PRAGMA user_version = 1;",
            )?,
            1..=5 => {}
            _ => return Err(Error::Invalid("queue schema is newer than this service")),
        }
        if version < 2 {
            migration.execute_batch(
                "CREATE TABLE transfer_journal (
                     job_id INTEGER PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
                     nonce TEXT NOT NULL UNIQUE,
                     identity BLOB NOT NULL CHECK (length(identity) = 48),
                     durable_bytes INTEGER NOT NULL DEFAULT 0 CHECK (durable_bytes >= 0),
                     total_bytes INTEGER,
                     etag BLOB,
                     prepared_bytes INTEGER CHECK (prepared_bytes >= 0),
                     CHECK (
                         (durable_bytes = 0 AND total_bytes IS NULL AND etag IS NULL)
                         OR (durable_bytes > 0 AND total_bytes IS NOT NULL AND total_bytes > durable_bytes AND etag IS NOT NULL)
                     ),
                     CHECK (prepared_bytes IS NULL OR prepared_bytes >= durable_bytes)
                 );
                 PRAGMA user_version = 2;",
            )?;
        }
        if version < 3 {
            migration.execute_batch(
                "CREATE TABLE retired_stages (
                    nonce TEXT PRIMARY KEY,
                    job_id INTEGER NOT NULL REFERENCES jobs(id),
                    identity BLOB NOT NULL,
                    output TEXT NOT NULL
                );
                PRAGMA user_version = 3;",
            )?;
        }
        if version < 4 {
            migration.execute_batch(
                "ALTER TABLE jobs ADD COLUMN stop_intent TEXT CHECK (stop_intent IN ('pause', 'cancel'));
                 PRAGMA user_version = 4;",
            )?;
        }
        if version < 5 {
            migration.execute_batch(
                "ALTER TABLE jobs ADD COLUMN sha256 TEXT;
                 PRAGMA user_version = 5;",
            )?;
        }
        migration.commit()?;
        Ok(Self(db))
    }

    pub fn claim(&mut self) -> Result<Option<Job>> {
        let job = self
            .0
            .query_row(
                "UPDATE jobs SET state = 'running'
                 WHERE id = (SELECT id FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1)
                 RETURNING id, url, output, sha256",
                [],
                |row| {
                    Ok(Job {
                        id: row.get(0)?,
                        url: row.get(1)?,
                        output: row.get(2)?,
                        digest: row
                            .get::<_, Option<String>>(3)?
                            .map(|value| {
                                value
                                    .parse()
                                    .map_err(|error: download_engine::DownloadError| {
                                        rusqlite::Error::FromSqlConversionFailure(
                                            3,
                                            rusqlite::types::Type::Text,
                                            Box::new(error),
                                        )
                                    })
                            })
                            .transpose()?,
                    })
                },
            )
            .optional()?;
        Ok(job)
    }

    pub fn recover(&mut self) -> Result<()> {
        // An old worker can leave jobs without any trusted staged data. Only
        // recorded prefixes or complete prepared payloads are safe to replay.
        self.0.execute(
            "UPDATE jobs SET state = CASE WHEN stop_intent IS NULL AND ?1 AND EXISTS (
                 SELECT 1 FROM transfer_journal AS journal WHERE journal.job_id = jobs.id
                 AND (journal.durable_bytes > 0 OR journal.prepared_bytes IS NOT NULL)
             ) THEN 'queued' ELSE 'interrupted' END WHERE state = 'running'",
            [cfg!(unix)],
        )?;
        Ok(())
    }

    pub fn finish(&mut self, id: i64, bytes: Option<u64>) -> Result<()> {
        let bytes = bytes.map(sql_size).transpose()?;
        let state = if bytes.is_some() {
            "completed"
        } else {
            "failed"
        };
        self.0.execute(
            "UPDATE jobs SET state = CASE WHEN stop_intent IS NULL THEN ?1 ELSE 'interrupted' END, bytes = ?2 WHERE id = ?3 AND state = 'running'",
            params![state, bytes, id],
        )?;
        Ok(())
    }

    pub fn retry(&mut self, request: Retry, key: Uuid) -> Result<Value> {
        let job_id = request.job_id;
        self.mutate(key, "job.retry", &serde_json::to_vec(&request)?, |tx| {
            let affected = tx.execute(
                "UPDATE jobs SET state = 'queued' WHERE id = ?1 AND state IN ('failed', 'interrupted') AND stop_intent IS NULL",
                [job_id.get()],
            )?;
            if affected == 0 {
                return Err(Error::InvalidState);
            }
            if request.restart {
                // Keep ownership evidence for later cleanup. A replaced file must
                // never be deleted merely because it occupied the old staging path.
                tx.execute(
                    "INSERT INTO retired_stages (nonce, job_id, identity, output)
                     SELECT nonce, job_id, identity, output FROM transfer_journal
                     JOIN jobs ON jobs.id = job_id
                     WHERE job_id = ?1 AND prepared_bytes IS NULL",
                    [job_id.get()],
                )?;
                tx.execute(
                    "DELETE FROM transfer_journal WHERE job_id = ?1 AND prepared_bytes IS NULL",
                    [job_id.get()],
                )?;
            }
            Ok(serde_json::to_value(Retried { job_id, phase: Phase::Queued })?)
        })
    }

    #[cfg(unix)]
    pub fn should_stop(&mut self, id: i64) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT stop_intent IS NOT NULL FROM jobs WHERE id = ?1",
            [id],
            |row| row.get(0),
        )?)
    }

    #[cfg(unix)]
    pub fn control(&mut self, action: JobAction, id: JobId, key: Uuid) -> Result<Value> {
        self.mutate(key, action.command_name(), &id.get().to_le_bytes(), |tx| {
            let sql = match action {
                JobAction::Pause => "UPDATE jobs SET stop_intent = 'pause',
                    state = CASE WHEN state = 'queued' THEN 'interrupted' ELSE state END
                    WHERE id = ?1 AND stop_intent IS NULL AND state IN ('queued', 'running')
                    AND NOT EXISTS (SELECT 1 FROM transfer_journal WHERE job_id = ?1 AND prepared_bytes IS NOT NULL)",
                JobAction::Resume => "UPDATE jobs SET stop_intent = NULL, state = 'queued'
                    WHERE id = ?1 AND state = 'interrupted' AND stop_intent = 'pause'",
                JobAction::Cancel => "UPDATE jobs SET stop_intent = 'cancel',
                    state = CASE WHEN state = 'running' THEN state ELSE 'interrupted' END
                    WHERE id = ?1 AND state != 'completed' AND stop_intent IS NOT 'cancel'
                    AND NOT EXISTS (SELECT 1 FROM transfer_journal WHERE job_id = ?1 AND prepared_bytes IS NOT NULL)",
            };
            let result = tx.query_row(
                &format!("{sql} RETURNING state, stop_intent"), [id.get()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            ).optional()?;
            let (state, intent) = result.ok_or(Error::Invalid("job cannot accept this action in its current state"))?;
            let phase = job_phase(&state, intent.as_deref())?;
            Ok(serde_json::to_value(JobTransition { job_id: id, phase })?)
        })
    }

    pub fn add(&mut self, request: Add, key: Uuid) -> Result<Value> {
        if request.url.len() > 8 * 1024 || request.output.len() > 16 * 1024 {
            return Err(Error::Invalid("download URL or destination is too long"));
        }
        if let Some(value) = &request.sha256 {
            value.parse::<download_engine::Sha256>().map_err(|_| {
                Error::Invalid("expected a SHA-256 digest of 64 hexadecimal characters")
            })?;
        }
        self.mutate(key, "job.add", &serde_json::to_vec(&request)?, |tx| {
            let source = reqwest::Url::parse(&request.url)
                .map_err(|_| Error::Invalid("invalid download URL"))?;
            if !matches!(source.scheme(), "http" | "https")
                || source.host().is_none()
                || !source.username().is_empty()
                || source.password().is_some()
                || source.query().is_some()
                || source.fragment().is_some()
            {
                return Err(Error::Invalid(
                    "queue requires a public HTTP(S) URL without credentials, query or fragment",
                ));
            }
            let path = Path::new(&request.output);
            let name = path
                .file_name()
                .filter(|_| path.is_absolute())
                .ok_or(Error::Invalid("expected an absolute destination file path"))?;
            let parent = path
                .parent()
                .and_then(|parent| parent.canonicalize().ok())
                .filter(|parent| parent.is_dir())
                .ok_or(Error::Invalid("destination directory is unavailable"))?;
            let path = parent.join(name);
            let output = path
                .to_str()
                .ok_or(Error::Invalid("queue requires a UTF-8 destination"))?;
            if tx.query_row("SELECT count(*) FROM jobs", [], |row| row.get::<_, i64>(0))?
                >= MAX_JOBS
            {
                return Err(Error::QueueFull);
            }
            tx.execute(
                "INSERT INTO jobs(url, output, state, sha256) VALUES (?1, ?2, 'queued', ?3)",
                params![source.as_str(), output, request.sha256],
            )?;
            let job_id = JobId::new(tx.last_insert_rowid()).ok_or(Error::Unavailable)?;
            Ok(serde_json::to_value(Added { job_id })?)
        })
    }

    fn mutate(
        &mut self,
        key: Uuid,
        command: &str,
        payload: &[u8],
        apply: impl FnOnce(&Transaction<'_>) -> Result<Value>,
    ) -> Result<Value> {
        let mut hasher = ring::digest::Context::new(&SHA256);
        hasher.update(command.as_bytes());
        hasher.update(&[0]);
        hasher.update(payload);
        let fingerprint = hasher.finish();
        let tx = self.0.transaction()?;
        tx.execute(
            "DELETE FROM command_receipts WHERE expires_at <= unixepoch()",
            [],
        )?;
        let key = key.hyphenated().to_string();
        let receipt: Option<(Vec<u8>, String)> = tx
            .query_row(
                "SELECT fingerprint, result_json FROM command_receipts WHERE key = ?1",
                [&key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let result = match receipt {
            Some((previous, result)) if previous == fingerprint.as_ref() => {
                serde_json::from_str(&result)?
            }
            Some(_) => return Err(Error::KeyConflict),
            None => {
                let result = apply(&tx)?;
                tx.execute(
                    "INSERT INTO command_receipts VALUES (?1, ?2, ?3, unixepoch() + 604800)",
                    params![key, fingerprint.as_ref(), serde_json::to_string(&result)?],
                )?;
                result
            }
        };
        tx.commit()?;
        Ok(result)
    }

    pub fn list(&mut self, after: Option<JobId>) -> Result<JobPage> {
        let mut statement = self.0.prepare(
            "SELECT jobs.id, jobs.state, jobs.output, jobs.bytes, journal.durable_bytes, jobs.stop_intent
             FROM jobs LEFT JOIN transfer_journal AS journal ON journal.job_id = jobs.id
             WHERE jobs.id > ?1 ORDER BY jobs.id LIMIT ?2",
        )?;
        let mut rows =
            statement.query(params![after.map_or(0, JobId::get), PAGE_ROWS as i64 + 1])?;
        let mut jobs = Vec::<JobSummary>::new();
        let mut size = 0;
        while let Some(row) = rows.next()? {
            let job = JobSummary {
                job_id: JobId::new(row.get(0)?).ok_or(Error::Unavailable)?,
                phase: job_phase(
                    &row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(5)?.as_deref(),
                )?,
                output: row.get(2)?,
                bytes: row.get::<_, Option<i64>>(3)?.map(|bytes| bytes.to_string()),
                durable_bytes: row.get::<_, Option<i64>>(4)?.map(|bytes| bytes.to_string()),
            };
            size += serde_json::to_vec(&job)?.len() + 1;
            if jobs.len() == PAGE_ROWS || size > PAGE_BYTES {
                let last = jobs.last().ok_or(Error::Unavailable)?;
                return Ok(JobPage {
                    next_cursor: Some(last.job_id),
                    jobs,
                });
            }
            jobs.push(job);
        }
        Ok(JobPage {
            jobs,
            next_cursor: None,
        })
    }
}

#[cfg(unix)]
pub use journal::Journal;
#[cfg(unix)]
mod journal;

fn job_phase(state: &str, stop_intent: Option<&str>) -> Result<Phase> {
    match (state, stop_intent) {
        ("queued", None) => Ok(Phase::Queued),
        ("running", None) => Ok(Phase::Running),
        ("completed", None) => Ok(Phase::Completed),
        ("failed", None) => Ok(Phase::Failed),
        ("interrupted", None) => Ok(Phase::Interrupted),
        ("running", Some("pause")) => Ok(Phase::Pausing),
        ("interrupted", Some("pause")) => Ok(Phase::Paused),
        ("running", Some("cancel")) => Ok(Phase::Canceling),
        ("interrupted", Some("cancel")) => Ok(Phase::Canceled),
        _ => Err(Error::Unavailable),
    }
}

fn sql_size(bytes: u64) -> Result<i64> {
    i64::try_from(bytes).map_err(|_| Error::Invalid("file size exceeds queue storage range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_restart_retires_missing_stage_and_preserves_receipt() {
        let dir = tempfile::tempdir().unwrap();
        let mut queue = Queue::open(&dir.path().join("queue.sqlite3")).unwrap();
        let output = dir.path().join("output").to_str().unwrap().to_owned();
        let added = queue
            .add(
                Add {
                    url: "https://example.com/file".into(),
                    output,
                    sha256: None,
                },
                Uuid::new_v4(),
            )
            .unwrap();
        let id = serde_json::from_value::<Added>(added).unwrap().job_id;
        queue
            .0
            .execute("UPDATE jobs SET state = 'failed' WHERE id = ?1", [id.get()])
            .unwrap();
        queue
            .0
            .execute(
                "INSERT INTO transfer_journal(job_id, nonce, identity)
             VALUES (?1, '0123456789abcdef0123456789abcdef', zeroblob(48))",
                [id.get()],
            )
            .unwrap();
        let key = Uuid::new_v4();
        let first = queue
            .retry(
                Retry {
                    job_id: id,
                    restart: true,
                },
                key,
            )
            .unwrap();
        let replay = queue
            .retry(
                Retry {
                    job_id: id,
                    restart: true,
                },
                key,
            )
            .unwrap();
        assert_eq!(first, replay);
        let active: i64 = queue
            .0
            .query_row("SELECT count(*) FROM transfer_journal", [], |r| r.get(0))
            .unwrap();
        let retired: i64 = queue
            .0
            .query_row("SELECT count(*) FROM retired_stages", [], |r| r.get(0))
            .unwrap();
        assert_eq!((active, retired), (0, 1));
        assert_eq!(queue.claim().unwrap().unwrap().id, id.get());
    }

    #[test]
    fn migrates_a_legacy_queue_and_keeps_its_ids_and_restart_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queue.sqlite3");
        let db = Connection::open(&path).unwrap();
        db.execute_batch(
            "CREATE TABLE jobs (
                id INTEGER PRIMARY KEY, url TEXT NOT NULL, output TEXT NOT NULL,
                state TEXT NOT NULL, bytes INTEGER
             );
             INSERT INTO jobs VALUES (7, 'https://example.com/first', '/file', 'running', NULL),
                                     (8, 'https://example.com/second', '/other', 'queued', NULL);",
        )
        .unwrap();
        drop(db);

        let mut queue = Queue::open(&path).unwrap();
        queue.recover().unwrap();
        let page = queue.list(None).unwrap();
        assert_eq!(page.jobs[0].job_id.get(), 7);
        assert!(matches!(page.jobs[0].phase, Phase::Interrupted));
        assert_eq!(queue.claim().unwrap().unwrap().id, 8);
        assert!(queue.claim().unwrap().is_none());
        let job_id = JobId::new(7).unwrap();
        queue
            .retry(
                Retry {
                    job_id,
                    restart: false,
                },
                Uuid::new_v4(),
            )
            .unwrap();
        assert_eq!(queue.claim().unwrap().unwrap().id, 7);
    }
}
