use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::unix::ffi::OsStrExt,
    path::Path,
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use tokio::sync::Notify;

pub struct ConfigReadBarrier {
    opened: Arc<Notify>,
    release: mpsc::Sender<()>,
    writer: thread::JoinHandle<()>,
}

impl ConfigReadBarrier {
    pub fn install(path: &Path) -> Self {
        let contents = fs::read(path).unwrap();
        fs::remove_file(path).unwrap();
        let path_c = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `path_c` is a live, NUL-terminated path and `mkfifo` does not retain it.
        assert_eq!(unsafe { libc::mkfifo(path_c.as_ptr(), 0o600) }, 0);
        let opened = Arc::new(Notify::new());
        let opened_for_writer = Arc::clone(&opened);
        let path = path.to_owned();
        let (release, released) = mpsc::channel();
        let writer = thread::spawn(move || {
            let mut pipe = OpenOptions::new().write(true).open(path).unwrap();
            opened_for_writer.notify_one();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
            pipe.write_all(&contents).unwrap();
        });
        Self {
            opened,
            release,
            writer,
        }
    }

    pub async fn wait_until_reader_opened(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.opened.notified())
            .await
            .expect("config reader must reach the pre-admission barrier");
    }

    pub fn release(self) {
        self.release.send(()).unwrap();
        self.writer.join().unwrap();
    }
}
