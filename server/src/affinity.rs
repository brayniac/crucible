//! CPU affinity utilities for pinning threads to specific cores.

/// Set CPU affinity for the current thread.
///
/// On Linux, this uses `sched_setaffinity` to pin the thread to a specific CPU.
/// On other platforms, this is a no-op.
#[cfg(target_os = "linux")]
pub fn set_cpu_affinity(cpu_id: usize) -> Result<(), String> {
    use std::mem;

    unsafe {
        let mut cpu_set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpu_set);
        libc::CPU_SET(cpu_id, &mut cpu_set);

        let result = libc::sched_setaffinity(0, mem::size_of::<libc::cpu_set_t>(), &cpu_set);

        if result == 0 {
            Ok(())
        } else {
            Err(format!(
                "sched_setaffinity failed with error code {}",
                result
            ))
        }
    }
}

/// Set CPU affinity for the current thread (no-op on non-Linux platforms).
#[cfg(not(target_os = "linux"))]
pub fn set_cpu_affinity(_cpu_id: usize) -> Result<(), String> {
    Ok(())
}
