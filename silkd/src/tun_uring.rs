//! io_uring packet path: tun reads stay queued in the kernel, and one submission carries many tun writes or one batched stream write.

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;

use io_uring::{IoUring, opcode, squeue, types};

use crate::tun::{MAX_FRAME, VNET_HDR_LEN, compact, next_frame};

const ENTRIES: u32 = 256;
/// One stream read of minimal frames queues about 7,700 tun writes; the completion queue holds them all.
const CQ_ENTRIES: u32 = 8192;
/// Tun reads kept queued, one frame per slot.
const SLOTS: usize = 32;
const DOWN_BUF: usize = 256 << 10;

const TUN_READ: u64 = 1 << 32;
const TUN_WRITE: u64 = 2 << 32;
const STREAM_READ: u64 = 3 << 32;
const STREAM_WRITE: u64 = 4 << 32;
const CANCEL: u64 = 5 << 32;
const KIND_MASK: u64 = !0xffff_ffff;

/// A ring set up before the session commits to io_uring, so a refusing kernel falls back to epoll.
pub struct Ring(IoUring);

impl Ring {
    pub fn new() -> io::Result<Ring> {
        Ok(Ring(
            IoUring::builder().setup_cqsize(CQ_ENTRIES).build(ENTRIES)?,
        ))
    }

    /// Moves frames between blocking `tun` and `stream` fds until either fails or the stream ends.
    pub fn pump(self, tun: RawFd, stream: RawFd) -> io::Result<()> {
        let mut p = Pump {
            ring: self.0,
            inflight: 0,
            tun,
            stream,
            slots: (0..SLOTS)
                .map(|_| vec![0u8; 4 + MAX_FRAME].into_boxed_slice())
                .collect(),
            ready: VecDeque::with_capacity(SLOTS),
            writing: Vec::with_capacity(SLOTS),
            iov: Vec::with_capacity(SLOTS),
            iov_at: 0,
            down: vec![0u8; DOWN_BUF].into_boxed_slice(),
            start: 0,
            end: 0,
            tun_writes: 0,
            done: Vec::with_capacity(ENTRIES as usize),
        };
        p.run()
    }
}

/// One session's ring and every buffer its queued operations name.
struct Pump {
    ring: IoUring,
    inflight: usize,
    tun: RawFd,
    stream: RawFd,

    slots: Vec<Box<[u8]>>,
    ready: VecDeque<(usize, usize)>,
    writing: Vec<usize>,
    iov: Vec<libc::iovec>,
    iov_at: usize,

    down: Box<[u8]>,
    start: usize,
    end: usize,
    tun_writes: usize,

    done: Vec<(u64, i32)>,
}

impl Pump {
    fn run(&mut self) -> io::Result<()> {
        for i in 0..SLOTS {
            self.arm_tun_read(i)?;
        }
        self.arm_stream_read()?;
        loop {
            match self.ring.submit_and_wait(1) {
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                res => res?,
            };
            self.done.clear();
            self.done
                .extend(self.ring.completion().map(|c| (c.user_data(), c.result())));
            self.inflight -= self.done.len();
            for i in 0..self.done.len() {
                let (ud, res) = self.done[i];
                let id = (ud & !KIND_MASK) as usize;
                match ud & KIND_MASK {
                    TUN_READ => self.tun_read_done(id, res)?,
                    TUN_WRITE => self.tun_write_done()?,
                    STREAM_READ => {
                        if !self.stream_read_done(res)? {
                            return Ok(());
                        }
                    }
                    STREAM_WRITE => self.stream_write_done(res)?,
                    _ => {}
                }
            }
            self.flush_ready()?;
        }
    }

    fn tun_read_done(&mut self, slot: usize, res: i32) -> io::Result<()> {
        if res >= VNET_HDR_LEN as i32 {
            let n = res as usize;
            self.slots[slot][..4].copy_from_slice(&(n as u32).to_le_bytes());
            self.ready.push_back((slot, 4 + n));
            return Ok(());
        }
        if res < 0 && !retryable(res) {
            return Err(io::Error::from_raw_os_error(-res));
        }
        self.arm_tun_read(slot)
    }

    fn tun_write_done(&mut self) -> io::Result<()> {
        self.tun_writes -= 1;
        if self.tun_writes == 0 {
            self.compact_and_read()?;
        }
        Ok(())
    }

    /// Returns false once the host closed the stream.
    fn stream_read_done(&mut self, res: i32) -> io::Result<bool> {
        match res {
            0 => return Ok(false),
            r if r < 0 && retryable(r) => {
                self.arm_stream_read()?;
                return Ok(true);
            }
            r if r < 0 => return Err(io::Error::from_raw_os_error(-r)),
            r => self.end += r as usize,
        }
        while let Some(len) = next_frame(&self.down[self.start..self.end])? {
            let pkt = self.down[self.start + 4..].as_ptr();
            let write = opcode::Write::new(types::Fd(self.tun), pkt, len as u32)
                .build()
                .user_data(TUN_WRITE);
            self.push(write)?;
            self.tun_writes += 1;
            self.start += 4 + len;
        }
        if self.tun_writes == 0 {
            self.compact_and_read()?;
        }
        Ok(true)
    }

    fn stream_write_done(&mut self, res: i32) -> io::Result<()> {
        if res < 0 {
            if retryable(res) {
                return self.submit_writev();
            }
            return Err(io::Error::from_raw_os_error(-res));
        }
        let mut n = res as usize;
        while n > 0 && self.iov_at < self.iov.len() {
            let v = &mut self.iov[self.iov_at];
            if n >= v.iov_len {
                n -= v.iov_len;
                self.iov_at += 1;
            } else {
                v.iov_base = v.iov_base.wrapping_byte_add(n);
                v.iov_len -= n;
                n = 0;
            }
        }
        if self.iov_at < self.iov.len() {
            return self.submit_writev();
        }
        while let Some(slot) = self.writing.pop() {
            self.arm_tun_read(slot)?;
        }
        Ok(())
    }

    fn flush_ready(&mut self) -> io::Result<()> {
        if !self.writing.is_empty() || self.ready.is_empty() {
            return Ok(());
        }
        self.iov.clear();
        self.iov_at = 0;
        while let Some((slot, len)) = self.ready.pop_front() {
            self.writing.push(slot);
            self.iov.push(libc::iovec {
                iov_base: self.slots[slot].as_mut_ptr().cast(),
                iov_len: len,
            });
        }
        self.submit_writev()
    }

    fn submit_writev(&mut self) -> io::Result<()> {
        let rest = &self.iov[self.iov_at..];
        let writev = opcode::Writev::new(types::Fd(self.stream), rest.as_ptr(), rest.len() as u32)
            .build()
            .user_data(STREAM_WRITE);
        self.push(writev)
    }

    fn arm_tun_read(&mut self, slot: usize) -> io::Result<()> {
        let buf = self.slots[slot][4..].as_mut_ptr();
        let read = opcode::Read::new(types::Fd(self.tun), buf, MAX_FRAME as u32)
            .build()
            .user_data(TUN_READ | slot as u64);
        self.push(read)
    }

    fn compact_and_read(&mut self) -> io::Result<()> {
        (self.start, self.end) = compact(&mut self.down, self.start, self.end);
        self.arm_stream_read()
    }

    fn arm_stream_read(&mut self) -> io::Result<()> {
        let rest = &mut self.down[self.end..];
        let read = opcode::Read::new(types::Fd(self.stream), rest.as_mut_ptr(), rest.len() as u32)
            .build()
            .user_data(STREAM_READ);
        self.push(read)
    }

    fn push(&mut self, entry: squeue::Entry) -> io::Result<()> {
        queue(&mut self.ring, &entry)?;
        self.inflight += 1;
        Ok(())
    }
}

impl Drop for Pump {
    fn drop(&mut self) {
        let targets = (0..SLOTS as u64)
            .map(|i| TUN_READ | i)
            .chain([STREAM_READ, STREAM_WRITE]);
        for ud in targets {
            let cancel = opcode::AsyncCancel::new(ud).build().user_data(CANCEL);
            if queue(&mut self.ring, &cancel).is_err() {
                break;
            }
        }
        while self.inflight > 0 {
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                // the ring cannot report completions; leaking the buffers beats freeing memory the kernel may write.
                Err(_) => {
                    std::mem::forget(std::mem::take(&mut self.slots));
                    std::mem::forget(std::mem::take(&mut self.iov));
                    std::mem::forget(std::mem::take(&mut self.down));
                    return;
                }
            }
            let finished = self
                .ring
                .completion()
                .filter(|c| c.user_data() != CANCEL)
                .count();
            self.inflight -= finished;
        }
    }
}

/// Pushes entry, submitting to make room while the queue is full.
fn queue(ring: &mut IoUring, entry: &squeue::Entry) -> io::Result<()> {
    // SAFETY: every buffer an entry names lives in its Pump, and Pump's Drop waits out each queued entry before freeing them.
    while unsafe { ring.submission().push(entry) }.is_err() {
        ring.submit()?;
    }
    Ok(())
}

fn retryable(res: i32) -> bool {
    res == -libc::EAGAIN || res == -libc::EINTR
}
