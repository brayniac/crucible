//! Hugepage allocation support for large memory regions.
//!
//! This module provides mmap-based allocation with hugepage support for Linux.
//! On non-Linux platforms, falls back to regular mmap.
//!
//! Optionally supports NUMA memory binding via `mbind()`.

use std::ptr::NonNull;

const KB: usize = 1024;
const MB: usize = 1024 * KB;
const GB: usize = 1024 * MB;

/// Hugepage size preference for large allocations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HugepageSize {
    /// No explicit hugepages, use regular 4KB pages.
    /// The OS may still use THP if configured system-wide.
    #[default]
    None,
    /// 2MB hugepages (MAP_HUGETLB | MAP_HUGE_2MB).
    /// Falls back to regular pages (with THP hint) if unavailable.
    TwoMegabyte,
    /// 1GB hugepages (MAP_HUGETLB | MAP_HUGE_1GB).
    /// Falls back directly to regular pages (with THP hint) if unavailable.
    OneGigabyte,
}

/// Result of a hugepage allocation attempt.
#[derive(Debug)]
pub struct HugepageAllocation {
    /// Pointer to the allocated memory (page-aligned).
    ptr: NonNull<u8>,
    /// Requested size before rounding.
    requested_size: usize,
    /// Actual allocated size (may be larger due to rounding).
    allocated_size: usize,
    /// The page size that was successfully used.
    page_size: AllocatedPageSize,
}

/// The actual page size used for an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatedPageSize {
    /// 1GB hugepages were used.
    OneGigabyte,
    /// 2MB hugepages were used.
    TwoMegabyte,
    /// Regular 4KB pages (possibly with THP).
    Regular,
}

impl std::fmt::Display for AllocatedPageSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocatedPageSize::OneGigabyte => write!(f, "1GB hugepages"),
            AllocatedPageSize::TwoMegabyte => write!(f, "2MB hugepages"),
            AllocatedPageSize::Regular => write!(f, "4KB pages"),
        }
    }
}

impl HugepageAllocation {
    /// Get a pointer to the allocated memory.
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Get the requested size (before rounding).
    pub fn requested_size(&self) -> usize {
        self.requested_size
    }

    /// Get the actual allocated size (after rounding).
    pub fn allocated_size(&self) -> usize {
        self.allocated_size
    }

    /// Get the page size that was used.
    pub fn page_size(&self) -> AllocatedPageSize {
        self.page_size
    }
}

impl Drop for HugepageAllocation {
    fn drop(&mut self) {
        unsafe {
            let result = libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.allocated_size);
            debug_assert_eq!(result, 0, "munmap failed");
        }
    }
}

// Safety: The allocation is just raw memory, safe to send between threads.
unsafe impl Send for HugepageAllocation {}
unsafe impl Sync for HugepageAllocation {}

/// Round up to the nearest multiple of `align`.
#[inline]
fn round_up(size: usize, align: usize) -> usize {
    (size + align - 1) & !(align - 1)
}

/// Format bytes as human-readable string.
fn format_bytes(bytes: usize) -> String {
    if bytes >= GB && bytes % GB == 0 {
        format!("{} GB", bytes / GB)
    } else if bytes >= MB && bytes % MB == 0 {
        format!("{} MB", bytes / MB)
    } else if bytes >= KB && bytes % KB == 0 {
        format!("{} KB", bytes / KB)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Allocate memory with the specified hugepage preference.
///
/// # Arguments
/// * `size` - Minimum size to allocate (will be rounded up for alignment).
/// * `hugepage_size` - Preferred hugepage size.
///
/// # Returns
/// * `Ok(HugepageAllocation)` - Successfully allocated memory.
/// * `Err(std::io::Error)` - All allocation attempts failed.
///
/// # Fallback Behavior
/// - `OneGigabyte`: Tries 1GB -> regular pages (with THP hint)
/// - `TwoMegabyte`: Tries 2MB -> regular pages (with THP hint)
/// - `None`: Uses regular pages directly
pub fn allocate(
    size: usize,
    hugepage_size: HugepageSize,
) -> Result<HugepageAllocation, std::io::Error> {
    allocate_on_node(size, hugepage_size, None)
}

/// Allocate memory with the specified hugepage preference, optionally bound to a NUMA node.
///
/// # Arguments
/// * `size` - Minimum size to allocate (will be rounded up for alignment).
/// * `hugepage_size` - Preferred hugepage size.
/// * `numa_node` - Optional NUMA node to bind memory to (Linux only).
///
/// # Returns
/// * `Ok(HugepageAllocation)` - Successfully allocated memory.
/// * `Err(std::io::Error)` - All allocation attempts failed.
pub fn allocate_on_node(
    size: usize,
    hugepage_size: HugepageSize,
    numa_node: Option<u32>,
) -> Result<HugepageAllocation, std::io::Error> {
    if size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "cannot allocate zero bytes",
        ));
    }

    let alloc = match hugepage_size {
        HugepageSize::OneGigabyte => allocate_prefer_1gb(size),
        HugepageSize::TwoMegabyte => allocate_prefer_2mb(size),
        HugepageSize::None => allocate_regular(size),
    }?;

    // Bind to NUMA node if specified
    if let Some(node) = numa_node {
        bind_to_numa_node(alloc.as_ptr(), alloc.allocated_size(), node)?;
    }

    Ok(alloc)
}

/// Bind a memory region to a specific NUMA node.
///
/// Uses `mbind()` with `MPOL_BIND` policy to ensure memory is allocated
/// on the specified node.
#[cfg(target_os = "linux")]
fn bind_to_numa_node(ptr: *mut u8, size: usize, node: u32) -> Result<(), std::io::Error> {
    // MPOL_BIND = 2: Allocate on specific nodes only
    const MPOL_BIND: libc::c_int = 2;
    // MPOL_MF_MOVE = 2: Move existing pages to comply with policy
    const MPOL_MF_MOVE: libc::c_uint = 1 << 1;

    // Create a nodemask with just the specified node
    // nodemask is a bitmask where bit N means node N
    let mut nodemask: libc::c_ulong = 1 << node;

    let result = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            ptr as *mut libc::c_void,
            size,
            MPOL_BIND,
            &mut nodemask as *mut libc::c_ulong,
            // maxnode: number of bits in nodemask (must be > highest node + 1)
            (node + 2) as libc::c_ulong,
            MPOL_MF_MOVE,
        )
    };

    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn bind_to_numa_node(_ptr: *mut u8, _size: usize, _node: u32) -> Result<(), std::io::Error> {
    // NUMA binding not supported on non-Linux platforms
    Ok(())
}

/// Try to allocate with 1GB pages, falling back directly to regular pages.
fn allocate_prefer_1gb(size: usize) -> Result<HugepageAllocation, std::io::Error> {
    // Only use 1GB pages if the waste is reasonable (< 50%)
    let rounded_1gb = round_up(size, GB);
    let waste_1gb = rounded_1gb - size;

    if size >= GB && waste_1gb * 2 <= rounded_1gb {
        // Waste is acceptable, try 1GB pages
        match try_mmap_hugepage(rounded_1gb, GB) {
            Ok(alloc) => {
                eprintln!(
                    "Allocated {} using 1GB hugepages ({} pages)",
                    format_bytes(rounded_1gb),
                    rounded_1gb / GB
                );
                return Ok(HugepageAllocation {
                    ptr: alloc,
                    requested_size: size,
                    allocated_size: rounded_1gb,
                    page_size: AllocatedPageSize::OneGigabyte,
                });
            }
            Err(e) => {
                eprintln!(
                    "1GB hugepage allocation failed ({}), falling back to regular pages",
                    e
                );
            }
        }
    }

    // Fall back to regular pages (with THP hint), rounded to 2MB for alignment
    let rounded_2mb = round_up(size, 2 * MB);
    allocate_regular_internal(rounded_2mb, size)
}

/// Try to allocate with 2MB pages, falling back to regular.
fn allocate_prefer_2mb(size: usize) -> Result<HugepageAllocation, std::io::Error> {
    let rounded_2mb = round_up(size, 2 * MB);

    match try_mmap_hugepage(rounded_2mb, 2 * MB) {
        Ok(alloc) => {
            eprintln!(
                "Allocated {} using 2MB hugepages ({} pages)",
                format_bytes(rounded_2mb),
                rounded_2mb / (2 * MB)
            );
            return Ok(HugepageAllocation {
                ptr: alloc,
                requested_size: size,
                allocated_size: rounded_2mb,
                page_size: AllocatedPageSize::TwoMegabyte,
            });
        }
        Err(e) => {
            eprintln!(
                "2MB hugepage allocation failed ({}), falling back to regular pages",
                e
            );
        }
    }

    // Fall back to regular pages (still use 2MB size for THP friendliness)
    allocate_regular_internal(rounded_2mb, size)
}

/// Allocate with regular pages (may still benefit from THP).
fn allocate_regular(size: usize) -> Result<HugepageAllocation, std::io::Error> {
    // Round up to page size for alignment
    let rounded = round_up(size, 4096);
    allocate_regular_internal(rounded, size)
}

/// Internal helper to allocate with regular pages.
fn allocate_regular_internal(
    alloc_size: usize,
    requested_size: usize,
) -> Result<HugepageAllocation, std::io::Error> {
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            alloc_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }

    // Try to enable THP via madvise (Linux only, best-effort)
    #[cfg(target_os = "linux")]
    unsafe {
        // MADV_HUGEPAGE = 14
        let _ = libc::madvise(ptr, alloc_size, 14);
    }

    eprintln!(
        "Allocated {} using regular pages (with THP hint)",
        format_bytes(alloc_size)
    );

    // Pre-fault pages
    prefault(ptr as *mut u8, alloc_size, 4096);

    Ok(HugepageAllocation {
        ptr: unsafe { NonNull::new_unchecked(ptr as *mut u8) },
        requested_size,
        allocated_size: alloc_size,
        page_size: AllocatedPageSize::Regular,
    })
}

/// Try to allocate memory using explicit hugepages.
#[cfg(target_os = "linux")]
fn try_mmap_hugepage(size: usize, page_size: usize) -> Result<NonNull<u8>, std::io::Error> {
    // MAP_HUGETLB = 0x40000
    // MAP_HUGE_2MB = 21 << 26
    // MAP_HUGE_1GB = 30 << 26
    const MAP_HUGETLB: libc::c_int = 0x40000;
    const MAP_HUGE_SHIFT: libc::c_int = 26;

    let huge_flag = if page_size == GB {
        MAP_HUGETLB | (30 << MAP_HUGE_SHIFT)
    } else if page_size == 2 * MB {
        MAP_HUGETLB | (21 << MAP_HUGE_SHIFT)
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "unsupported hugepage size",
        ));
    };

    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | huge_flag,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }

    // Pre-fault hugepages
    prefault(ptr as *mut u8, size, page_size);

    Ok(unsafe { NonNull::new_unchecked(ptr as *mut u8) })
}

/// On non-Linux, hugepages are not supported.
#[cfg(not(target_os = "linux"))]
fn try_mmap_hugepage(_size: usize, _page_size: usize) -> Result<NonNull<u8>, std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "hugepages not supported on this platform",
    ))
}

/// Pre-fault all pages by touching each page.
/// This forces the OS to allocate physical pages immediately.
fn prefault(ptr: *mut u8, size: usize, page_size: usize) {
    unsafe {
        for offset in (0..size).step_by(page_size) {
            std::ptr::write_volatile(ptr.add(offset), 0);
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::*;

    #[test]
    fn test_round_up() {
        assert_eq!(round_up(0, 4096), 0);
        assert_eq!(round_up(1, 4096), 4096);
        assert_eq!(round_up(4096, 4096), 4096);
        assert_eq!(round_up(4097, 4096), 8192);
        assert_eq!(round_up(1, 2 * MB), 2 * MB);
        assert_eq!(round_up(2 * MB, 2 * MB), 2 * MB);
        assert_eq!(round_up(2 * MB + 1, 2 * MB), 4 * MB);
    }

    #[test]
    fn test_allocate_regular() {
        let alloc = allocate(1024 * 1024, HugepageSize::None).expect("allocation failed");
        assert!(alloc.allocated_size() >= 1024 * 1024);
        assert_eq!(alloc.requested_size(), 1024 * 1024);

        // Write to verify it's usable
        unsafe {
            std::ptr::write_volatile(alloc.as_ptr(), 42);
            assert_eq!(std::ptr::read_volatile(alloc.as_ptr()), 42);
        }
    }

    #[test]
    fn test_allocate_2mb_preference() {
        // This may or may not succeed with actual hugepages depending on system config
        let alloc = allocate(64 * MB, HugepageSize::TwoMegabyte).expect("allocation failed");
        assert!(alloc.allocated_size() >= 64 * MB);
        // Size should be rounded up to 2MB boundary
        assert_eq!(alloc.allocated_size() % (2 * MB), 0);
    }

    #[test]
    fn test_zero_size_fails() {
        let result = allocate(0, HugepageSize::None);
        assert!(result.is_err());
    }

    #[test]
    fn test_allocate_1gb_preference() {
        // Try to allocate with 1GB preference - will likely fall back to regular pages
        // but we're testing the code path
        let alloc = allocate(64 * MB, HugepageSize::OneGigabyte).expect("allocation failed");
        assert!(alloc.allocated_size() >= 64 * MB);
    }

    #[test]
    fn test_hugepage_size_default() {
        let size = HugepageSize::default();
        assert_eq!(size, HugepageSize::None);
    }

    #[test]
    fn test_hugepage_size_equality() {
        assert_eq!(HugepageSize::None, HugepageSize::None);
        assert_eq!(HugepageSize::TwoMegabyte, HugepageSize::TwoMegabyte);
        assert_eq!(HugepageSize::OneGigabyte, HugepageSize::OneGigabyte);
        assert_ne!(HugepageSize::None, HugepageSize::TwoMegabyte);
        assert_ne!(HugepageSize::TwoMegabyte, HugepageSize::OneGigabyte);
    }

    #[test]
    fn test_hugepage_size_clone() {
        let size = HugepageSize::TwoMegabyte;
        let cloned = size;
        assert_eq!(size, cloned);
    }

    #[test]
    fn test_hugepage_size_debug() {
        let debug_str = format!("{:?}", HugepageSize::TwoMegabyte);
        assert!(debug_str.contains("TwoMegabyte"));
    }

    #[test]
    fn test_allocated_page_size_equality() {
        assert_eq!(AllocatedPageSize::Regular, AllocatedPageSize::Regular);
        assert_eq!(
            AllocatedPageSize::TwoMegabyte,
            AllocatedPageSize::TwoMegabyte
        );
        assert_eq!(
            AllocatedPageSize::OneGigabyte,
            AllocatedPageSize::OneGigabyte
        );
        assert_ne!(AllocatedPageSize::Regular, AllocatedPageSize::TwoMegabyte);
    }

    #[test]
    fn test_allocated_page_size_clone() {
        let size = AllocatedPageSize::Regular;
        let cloned = size;
        assert_eq!(size, cloned);
    }

    #[test]
    fn test_allocated_page_size_debug() {
        let debug_str = format!("{:?}", AllocatedPageSize::Regular);
        assert!(debug_str.contains("Regular"));
    }

    #[test]
    fn test_allocation_page_size() {
        let alloc = allocate(1024 * 1024, HugepageSize::None).expect("allocation failed");
        let page_size = alloc.page_size();
        // Regular allocation should use regular pages
        assert_eq!(page_size, AllocatedPageSize::Regular);
    }

    #[test]
    fn test_allocation_as_ptr() {
        let alloc = allocate(4096, HugepageSize::None).expect("allocation failed");
        let ptr = alloc.as_ptr();
        assert!(!ptr.is_null());
    }

    #[test]
    fn test_hugepage_allocation_debug() {
        let alloc = allocate(4096, HugepageSize::None).expect("allocation failed");
        let debug_str = format!("{:?}", alloc);
        assert!(debug_str.contains("HugepageAllocation"));
    }

    #[test]
    fn test_allocate_small_size() {
        // Allocate a very small size (will be rounded up to page size)
        let alloc = allocate(1, HugepageSize::None).expect("allocation failed");
        assert!(alloc.allocated_size() >= 4096); // At least one page
        assert_eq!(alloc.requested_size(), 1);
    }

    #[test]
    fn test_allocate_exact_page_size() {
        let alloc = allocate(4096, HugepageSize::None).expect("allocation failed");
        assert_eq!(alloc.allocated_size(), 4096);
        assert_eq!(alloc.requested_size(), 4096);
    }

    #[test]
    fn test_allocate_2mb_small_size() {
        // Request small size but prefer 2MB pages - should round up
        let alloc = allocate(1024, HugepageSize::TwoMegabyte).expect("allocation failed");
        assert!(alloc.allocated_size() >= 2 * MB);
    }

    #[test]
    fn test_allocate_1gb_small_size() {
        // Request small size but prefer 1GB pages - should fall back to regular
        let alloc = allocate(1024, HugepageSize::OneGigabyte).expect("allocation failed");
        // Will fall back to regular pages since waste would be too high
        assert!(alloc.allocated_size() >= 1024);
    }

    #[test]
    fn test_multiple_allocations() {
        // Allocate multiple regions
        let alloc1 = allocate(1024 * 1024, HugepageSize::None).expect("allocation failed");
        let alloc2 = allocate(1024 * 1024, HugepageSize::None).expect("allocation failed");

        // Should be different allocations
        assert_ne!(alloc1.as_ptr(), alloc2.as_ptr());
    }
}
