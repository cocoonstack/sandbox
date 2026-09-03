//! Thin wrappers over the syscalls the boot path needs; all the unsafe lives here.

use std::ffi::CString;
use std::io;
use std::ptr;
use std::time::Duration;

/// nosuid|nodev|noexec for the kernel API filesystems.
pub const MNT_SECURE: libc::c_ulong = libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC;

pub fn mount(
    src: &str,
    target: &str,
    fstype: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> Result<(), String> {
    let src_c = cstr(src)?;
    let target_c = cstr(target)?;
    let fstype_c = fstype.map(cstr).transpose()?;
    let data_c = data.map(cstr).transpose()?;
    // SAFETY: every pointer is either a live CString for the call's duration
    // or null (the optional fstype/data), exactly as mount(2) expects.
    let ret = unsafe {
        libc::mount(
            src_c.as_ptr(),
            target_c.as_ptr(),
            fstype_c.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
            flags,
            data_c.as_ref().map_or(ptr::null(), |c| c.as_ptr()) as *const libc::c_void,
        )
    };
    if ret != 0 {
        return Err(format!(
            "mount {src} -> {target}: {}",
            io::Error::last_os_error()
        ));
    }
    Ok(())
}

pub fn move_mount(src: &str, target: &str) -> Result<(), String> {
    mount(src, target, None, libc::MS_MOVE, None)
}

pub fn sethostname(name: &str) -> Result<(), String> {
    // SAFETY: name is a live &str; sethostname(2) copies name.len() bytes.
    let ret = unsafe { libc::sethostname(name.as_ptr() as *const libc::c_char, name.len()) };
    if ret != 0 {
        return Err(format!(
            "sethostname {name}: {}",
            io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// Re-roots into the assembled overlay; the ~1MiB initramfs is deliberately never freed.
pub fn switch_root(newroot: &str) -> Result<(), String> {
    std::env::set_current_dir(newroot).map_err(|err| format!("chdir {newroot}: {err}"))?;
    move_mount(".", "/")?;
    // SAFETY: the literal is a live 'static CStr for the duration of the call.
    if unsafe { libc::chroot(c".".as_ptr()) } != 0 {
        return Err(format!("chroot: {}", io::Error::last_os_error()));
    }
    std::env::set_current_dir("/").map_err(|err| format!("chdir /: {err}"))
}

/// Only returns on failure.
pub fn exec_init(path: &str) -> String {
    let Ok(path_c) = CString::new(path) else {
        return format!("invalid init path {path:?}");
    };
    let argv = [path_c.as_ptr(), ptr::null()];
    let envp = [
        c"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".as_ptr(),
        c"TERM=linux".as_ptr(),
        ptr::null(),
    ];
    // SAFETY: path_c, argv, and envp are live and NUL-terminated for the call;
    // execve only returns on failure.
    unsafe { libc::execve(path_c.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
    io::Error::last_os_error().to_string()
}

/// Routes PID 1 stdio to /dev/console, covering a cpio built without the console node.
pub fn claim_console() {
    // SAFETY: the literal is a live 'static CStr; the fd is validated (>=0)
    // before any dup2/close and only closed when it is not already a std stream.
    unsafe {
        let fd = libc::open(c"/dev/console".as_ptr(), libc::O_RDWR);
        if fd >= 0 {
            libc::dup2(fd, 0);
            libc::dup2(fd, 1);
            libc::dup2(fd, 2);
            if fd > 2 {
                libc::close(fd);
            }
        }
    }
}

/// Terminal state: hands the operator a debug shell, else powers off so the VM dies visibly.
pub fn fatal(msg: &str, debug: bool) -> ! {
    eprintln!("sandbox-init: FATAL: {msg}");
    if debug {
        let argv = [c"/bin/sh".as_ptr(), ptr::null()];
        // SAFETY: the literal and argv are live and NUL-terminated for the call.
        unsafe { libc::execv(c"/bin/sh".as_ptr(), argv.as_ptr()) };
        eprintln!("sandbox-init: no /bin/sh in this initramfs (non-debug build?)");
    }
    // SAFETY: sync(2) and reboot(2) take no pointer arguments.
    unsafe {
        libc::sync();
        libc::reboot(libc::RB_POWER_OFF);
    }
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

fn cstr(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| format!("NUL byte in {s:?}"))
}
