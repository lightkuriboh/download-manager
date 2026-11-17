#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::{path::PathBuf, time::Duration};

use clap::Parser;
#[cfg(unix)]
use download_protocol::JobAction;
use download_protocol::{JobId, JobPage, Phase, Uuid};
use iced::{
    Element, Fill, Subscription, Task,
    widget::{button, column, container, row, scrollable, text, text_input},
};

mod queue;
use queue::{Change, Endpoint};

#[derive(Parser)]
#[command(about = "Desktop client for the download manager queue")]
struct Args {
    #[arg(long)]
    state_dir: PathBuf,
    /// Path to the matching download-managerd executable.
    #[cfg(windows)]
    #[arg(long)]
    service: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let endpoint = Endpoint {
        state_dir: std::path::absolute(args.state_dir)?,
        #[cfg(windows)]
        service: match args.service {
            Some(path) => std::path::absolute(path)?,
            None => std::env::current_exe()?.with_file_name("download-managerd.exe"),
        },
    };

    iced::application(
        move || Model::start(endpoint.clone()),
        Model::update,
        Model::view,
    )
    .title("Download Manager")
    .subscription(|_| iced::time::every(Duration::from_secs(2)).map(|_| Message::Refresh))
    .window_size((850.0, 600.0))
    .run()?;
    Ok(())
}

struct Model {
    endpoint: Endpoint,
    url: String,
    output: String,
    sha256: String,
    search: String,
    page: Option<JobPage>,
    history: Vec<Option<JobId>>,
    busy: bool,
    error: Option<String>,
    failed_change: Option<(Change, Uuid)>,
}

#[derive(Debug, Clone)]
enum Message {
    Url(String),
    Output(String),
    Digest(String),
    Search(String),
    Refresh,
    Next,
    Previous,
    Add,
    Change(Change),
    RetryCommand,
    Dismiss,
    Listed(Result<JobPage, String>),
    Changed(Change, Uuid, Result<(), queue::ChangeError>),
}

impl Model {
    fn start(endpoint: Endpoint) -> (Self, Task<Message>) {
        let mut model = Self {
            endpoint,
            url: String::new(),
            output: String::new(),
            sha256: String::new(),
            search: String::new(),
            page: None,
            history: vec![None],
            busy: false,
            error: None,
            failed_change: None,
        };
        let task = model.refresh();
        (model, task)
    }

    fn refresh(&mut self) -> Task<Message> {
        self.busy = true;
        let endpoint = self.endpoint.clone();
        let after = self.history.last().copied().flatten();
        Task::perform(async move { endpoint.list(after).await }, Message::Listed)
    }

    fn change(&mut self, change: Change, key: Uuid) -> Task<Message> {
        self.busy = true;
        self.error = None;
        self.failed_change = None;
        let endpoint = self.endpoint.clone();
        Task::perform(
            async move {
                let result = endpoint.change(change.clone(), key).await;
                (change, key, result)
            },
            |(change, key, result)| Message::Changed(change, key, result),
        )
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::Url(value) => self.url = value,
            Message::Output(value) => self.output = value,
            Message::Digest(value) => self.sha256 = value,
            Message::Search(value) => self.search = value,
            Message::Refresh if !self.busy => return self.refresh(),
            Message::Next if !self.busy => {
                if let Some(next) = self.page.as_ref().and_then(|page| page.next_cursor) {
                    self.history.push(Some(next));
                    self.page = None;
                    return self.refresh();
                }
            }
            Message::Previous if !self.busy && self.history.len() > 1 => {
                self.history.pop();
                self.page = None;
                return self.refresh();
            }
            Message::Add if !self.busy && self.failed_change.is_none() => {
                let result = std::path::absolute(self.output.trim())
                    .map_err(|error| error.to_string())
                    .and_then(|path| {
                        path.into_os_string()
                            .into_string()
                            .map_err(|_| "Use a UTF-8 destination path".into())
                    });
                match result {
                    Ok(output) if !self.url.trim().is_empty() && !self.output.trim().is_empty() => {
                        return self.change(
                            Change::Add {
                                output,
                                url: self.url.trim().into(),
                                sha256: (!self.sha256.trim().is_empty())
                                    .then(|| self.sha256.trim().into()),
                            },
                            Uuid::new_v4(),
                        );
                    }
                    Ok(_) => self.error = Some("Enter a URL and destination".into()),
                    Err(error) => self.error = Some(error),
                }
            }
            Message::Change(change) if !self.busy && self.failed_change.is_none() => {
                return self.change(change, Uuid::new_v4());
            }
            Message::RetryCommand if !self.busy => {
                if let Some((change, key)) = self.failed_change.take() {
                    return self.change(change, key);
                }
            }
            Message::Listed(result) => {
                self.busy = false;
                match result {
                    Ok(page) => {
                        self.page = Some(page);
                        if self.failed_change.is_none() {
                            self.error = None;
                        }
                    }
                    Err(error) if self.failed_change.is_none() => self.error = Some(error),
                    Err(_) => {}
                }
            }
            Message::Changed(change, key, result) => {
                self.busy = false;
                match result {
                    Ok(()) => {
                        if matches!(change, Change::Add { .. }) {
                            self.url.clear();
                            self.output.clear();
                            self.sha256.clear();
                        }
                        return self.refresh();
                    }
                    Err(queue::ChangeError::Rejected(error)) => self.error = Some(error),
                    Err(queue::ChangeError::Uncertain(error)) => {
                        self.error = Some(format!(
                            "{error}. Retry this command with the same request key, or dismiss it after checking the queue."
                        ));
                        self.failed_change = Some((change, key));
                    }
                }
            }
            Message::Dismiss => {
                self.failed_change = None;
                self.error = None;
            }
            _ => {}
        }
        Task::none()
    }

    fn view(&self) -> Element<'_, Message> {
        let searchable = self.search.to_lowercase();
        let can_change = !self.busy && self.failed_change.is_none();
        let mut jobs = column![].spacing(8);
        if let Some(page) = &self.page {
            for job in &page.jobs {
                if !job.output.to_lowercase().contains(&searchable) {
                    continue;
                }
                let size = job.bytes.as_deref().or(job.durable_bytes.as_deref());
                let subtitle = size.map_or_else(
                    || format!("#{}  {}", job.job_id, job.phase),
                    |bytes| format!("#{}  {}  •  {} bytes", job.job_id, job.phase, bytes),
                );
                let mut actions = row![].spacing(6);
                if can_change {
                    #[cfg(unix)]
                    match job.phase {
                        Phase::Queued | Phase::Running => {
                            actions =
                                actions
                                    .push(button("Pause").on_press(Message::Change(
                                        Change::Control(JobAction::Pause, job.job_id),
                                    )))
                                    .push(button("Cancel").on_press(Message::Change(
                                        Change::Control(JobAction::Cancel, job.job_id),
                                    )));
                        }
                        Phase::Paused => {
                            actions =
                                actions
                                    .push(button("Resume").on_press(Message::Change(
                                        Change::Control(JobAction::Resume, job.job_id),
                                    )))
                                    .push(button("Cancel").on_press(Message::Change(
                                        Change::Control(JobAction::Cancel, job.job_id),
                                    )));
                        }
                        _ => {}
                    }
                    if matches!(job.phase, Phase::Failed | Phase::Interrupted) {
                        actions = actions
                            .push(button("Retry").on_press(Message::Change(Change::Retry {
                                id: job.job_id,
                                restart: false,
                            })))
                            .push(
                                button("Start over").on_press(Message::Change(Change::Retry {
                                    id: job.job_id,
                                    restart: true,
                                })),
                            );
                    }
                }
                jobs = jobs.push(
                    row![
                        column![text(&job.output), text(subtitle).size(13)].width(Fill),
                        actions,
                    ]
                    .spacing(12)
                    .align_y(iced::Alignment::Center),
                );
            }
        }
        if self.page.as_ref().is_none_or(|page| {
            !page
                .jobs
                .iter()
                .any(|job| job.output.to_lowercase().contains(&searchable))
        }) {
            jobs = jobs.push(text(if self.page.is_none() && self.busy {
                "Loading queue…"
            } else {
                "No matching jobs"
            }));
        }

        let mut status = row![].spacing(8);
        if let Some(error) = &self.error {
            status = status.push(text(error).width(Fill));
        }
        if self.failed_change.is_some() {
            status = status
                .push(
                    button("Retry command")
                        .on_press_maybe((!self.busy).then_some(Message::RetryCommand)),
                )
                .push(button("Dismiss").on_press(Message::Dismiss));
        }

        container(
            column![
                text("Download Manager").size(28),
                text_input("Public HTTP(S) URL", &self.url).on_input(Message::Url),
                text_input("Destination file path", &self.output).on_input(Message::Output),
                row![
                    text_input("Expected SHA-256 (optional)", &self.sha256)
                        .on_input(Message::Digest),
                    button("Add download").on_press_maybe(can_change.then_some(Message::Add)),
                ]
                .spacing(8),
                status,
                text_input("Filter this page by destination", &self.search)
                    .on_input(Message::Search),
                scrollable(jobs).height(Fill),
                row![
                    button("Previous").on_press_maybe(
                        (!self.busy && self.history.len() > 1).then_some(Message::Previous)
                    ),
                    button("Next").on_press_maybe(
                        (!self.busy
                            && self
                                .page
                                .as_ref()
                                .is_some_and(|page| page.next_cursor.is_some()))
                        .then_some(Message::Next)
                    ),
                    button("Refresh").on_press_maybe((!self.busy).then_some(Message::Refresh)),
                ]
                .spacing(8),
            ]
            .spacing(10),
        )
        .padding(18)
        .into()
    }
}
