//! HPACK static and dynamic tables.

use std::collections::VecDeque;

/// A header field (name-value pair).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderField {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl HeaderField {
    /// Create a new header field.
    pub fn new(name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Create a header field from static strings.
    pub const fn from_static(name: &'static [u8], value: &'static [u8]) -> StaticEntry {
        StaticEntry { name, value }
    }

    /// Get the size of this header field for table accounting.
    /// Size = length of name + length of value + 32 (RFC 7541 Section 4.1)
    pub fn size(&self) -> usize {
        self.name.len() + self.value.len() + 32
    }
}

/// A static table entry (references static data).
#[derive(Debug, Clone, Copy)]
pub struct StaticEntry {
    pub name: &'static [u8],
    pub value: &'static [u8],
}

impl StaticEntry {
    /// Get the size of this header field for table accounting.
    pub fn size(&self) -> usize {
        self.name.len() + self.value.len() + 32
    }
}

/// The HPACK static table (RFC 7541 Appendix A).
///
/// Index 1-61, index 0 is invalid.
pub struct StaticTable;

impl StaticTable {
    /// Static table entries. Index 0 is unused (indices are 1-based).
    const ENTRIES: [StaticEntry; 62] = [
        // Index 0 - unused placeholder
        StaticEntry {
            name: b"",
            value: b"",
        },
        // Index 1
        StaticEntry {
            name: b":authority",
            value: b"",
        },
        // Index 2
        StaticEntry {
            name: b":method",
            value: b"GET",
        },
        // Index 3
        StaticEntry {
            name: b":method",
            value: b"POST",
        },
        // Index 4
        StaticEntry {
            name: b":path",
            value: b"/",
        },
        // Index 5
        StaticEntry {
            name: b":path",
            value: b"/index.html",
        },
        // Index 6
        StaticEntry {
            name: b":scheme",
            value: b"http",
        },
        // Index 7
        StaticEntry {
            name: b":scheme",
            value: b"https",
        },
        // Index 8
        StaticEntry {
            name: b":status",
            value: b"200",
        },
        // Index 9
        StaticEntry {
            name: b":status",
            value: b"204",
        },
        // Index 10
        StaticEntry {
            name: b":status",
            value: b"206",
        },
        // Index 11
        StaticEntry {
            name: b":status",
            value: b"304",
        },
        // Index 12
        StaticEntry {
            name: b":status",
            value: b"400",
        },
        // Index 13
        StaticEntry {
            name: b":status",
            value: b"404",
        },
        // Index 14
        StaticEntry {
            name: b":status",
            value: b"500",
        },
        // Index 15
        StaticEntry {
            name: b"accept-charset",
            value: b"",
        },
        // Index 16
        StaticEntry {
            name: b"accept-encoding",
            value: b"gzip, deflate",
        },
        // Index 17
        StaticEntry {
            name: b"accept-language",
            value: b"",
        },
        // Index 18
        StaticEntry {
            name: b"accept-ranges",
            value: b"",
        },
        // Index 19
        StaticEntry {
            name: b"accept",
            value: b"",
        },
        // Index 20
        StaticEntry {
            name: b"access-control-allow-origin",
            value: b"",
        },
        // Index 21
        StaticEntry {
            name: b"age",
            value: b"",
        },
        // Index 22
        StaticEntry {
            name: b"allow",
            value: b"",
        },
        // Index 23
        StaticEntry {
            name: b"authorization",
            value: b"",
        },
        // Index 24
        StaticEntry {
            name: b"cache-control",
            value: b"",
        },
        // Index 25
        StaticEntry {
            name: b"content-disposition",
            value: b"",
        },
        // Index 26
        StaticEntry {
            name: b"content-encoding",
            value: b"",
        },
        // Index 27
        StaticEntry {
            name: b"content-language",
            value: b"",
        },
        // Index 28
        StaticEntry {
            name: b"content-length",
            value: b"",
        },
        // Index 29
        StaticEntry {
            name: b"content-location",
            value: b"",
        },
        // Index 30
        StaticEntry {
            name: b"content-range",
            value: b"",
        },
        // Index 31
        StaticEntry {
            name: b"content-type",
            value: b"",
        },
        // Index 32
        StaticEntry {
            name: b"cookie",
            value: b"",
        },
        // Index 33
        StaticEntry {
            name: b"date",
            value: b"",
        },
        // Index 34
        StaticEntry {
            name: b"etag",
            value: b"",
        },
        // Index 35
        StaticEntry {
            name: b"expect",
            value: b"",
        },
        // Index 36
        StaticEntry {
            name: b"expires",
            value: b"",
        },
        // Index 37
        StaticEntry {
            name: b"from",
            value: b"",
        },
        // Index 38
        StaticEntry {
            name: b"host",
            value: b"",
        },
        // Index 39
        StaticEntry {
            name: b"if-match",
            value: b"",
        },
        // Index 40
        StaticEntry {
            name: b"if-modified-since",
            value: b"",
        },
        // Index 41
        StaticEntry {
            name: b"if-none-match",
            value: b"",
        },
        // Index 42
        StaticEntry {
            name: b"if-range",
            value: b"",
        },
        // Index 43
        StaticEntry {
            name: b"if-unmodified-since",
            value: b"",
        },
        // Index 44
        StaticEntry {
            name: b"last-modified",
            value: b"",
        },
        // Index 45
        StaticEntry {
            name: b"link",
            value: b"",
        },
        // Index 46
        StaticEntry {
            name: b"location",
            value: b"",
        },
        // Index 47
        StaticEntry {
            name: b"max-forwards",
            value: b"",
        },
        // Index 48
        StaticEntry {
            name: b"proxy-authenticate",
            value: b"",
        },
        // Index 49
        StaticEntry {
            name: b"proxy-authorization",
            value: b"",
        },
        // Index 50
        StaticEntry {
            name: b"range",
            value: b"",
        },
        // Index 51
        StaticEntry {
            name: b"referer",
            value: b"",
        },
        // Index 52
        StaticEntry {
            name: b"refresh",
            value: b"",
        },
        // Index 53
        StaticEntry {
            name: b"retry-after",
            value: b"",
        },
        // Index 54
        StaticEntry {
            name: b"server",
            value: b"",
        },
        // Index 55
        StaticEntry {
            name: b"set-cookie",
            value: b"",
        },
        // Index 56
        StaticEntry {
            name: b"strict-transport-security",
            value: b"",
        },
        // Index 57
        StaticEntry {
            name: b"transfer-encoding",
            value: b"",
        },
        // Index 58
        StaticEntry {
            name: b"user-agent",
            value: b"",
        },
        // Index 59
        StaticEntry {
            name: b"vary",
            value: b"",
        },
        // Index 60
        StaticEntry {
            name: b"via",
            value: b"",
        },
        // Index 61
        StaticEntry {
            name: b"www-authenticate",
            value: b"",
        },
    ];

    /// Get a static table entry by index (1-61).
    pub fn get(index: usize) -> Option<&'static StaticEntry> {
        if index == 0 || index > 61 {
            None
        } else {
            Some(&Self::ENTRIES[index])
        }
    }

    /// Find an entry in the static table.
    /// Returns (index, exact_match) where exact_match is true if both name and value match.
    pub fn find(name: &[u8], value: &[u8]) -> Option<(usize, bool)> {
        let mut name_match = None;

        for (i, entry) in Self::ENTRIES.iter().enumerate().skip(1) {
            if entry.name == name {
                if entry.value == value {
                    return Some((i, true));
                }
                if name_match.is_none() {
                    name_match = Some(i);
                }
            }
        }

        name_match.map(|i| (i, false))
    }

    /// Get the number of entries in the static table.
    pub const fn len() -> usize {
        61
    }
}

/// The HPACK dynamic table.
///
/// The dynamic table is a FIFO queue of header fields, with newest entries
/// at the front. Entries are evicted from the back when the table exceeds
/// its maximum size.
pub struct DynamicTable {
    /// Header entries, newest first.
    entries: VecDeque<HeaderField>,
    /// Current size in bytes.
    size: usize,
    /// Maximum size in bytes.
    max_size: usize,
}

impl DynamicTable {
    /// Create a new dynamic table with the given maximum size.
    pub(super) fn new(max_size: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            size: 0,
            max_size,
        }
    }

    /// Set the maximum size of the table, evicting entries as needed.
    pub(super) fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
        self.evict();
    }

    /// Get an entry by index (0 = newest entry).
    pub(super) fn get(&self, index: usize) -> Option<&HeaderField> {
        self.entries.get(index)
    }

    /// Insert a new entry at the front of the table.
    pub(super) fn insert(&mut self, field: HeaderField) {
        let entry_size = field.size();

        // If the entry is larger than the table, clear the table
        if entry_size > self.max_size {
            self.entries.clear();
            self.size = 0;
            return;
        }

        // Evict entries until there's room
        while self.size + entry_size > self.max_size {
            if let Some(evicted) = self.entries.pop_back() {
                self.size -= evicted.size();
            } else {
                break;
            }
        }

        self.entries.push_front(field);
        self.size += entry_size;
    }

    /// Find an entry in the dynamic table.
    /// Returns (index, exact_match) where index is 0-based within the dynamic table.
    pub(super) fn find(&self, name: &[u8], value: &[u8]) -> Option<(usize, bool)> {
        let mut name_match = None;

        for (i, entry) in self.entries.iter().enumerate() {
            if entry.name == name {
                if entry.value == value {
                    return Some((i, true));
                }
                if name_match.is_none() {
                    name_match = Some(i);
                }
            }
        }

        name_match.map(|i| (i, false))
    }

    /// Evict entries until the table is within its maximum size.
    fn evict(&mut self) {
        while self.size > self.max_size {
            if let Some(evicted) = self.entries.pop_back() {
                self.size -= evicted.size();
            } else {
                break;
            }
        }
    }

    /// Get the number of entries in the table.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Get the current size of the table in bytes.
    #[cfg(test)]
    fn size(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_table_get() {
        // Test known entries
        let entry = StaticTable::get(1).unwrap();
        assert_eq!(entry.name, b":authority");
        assert_eq!(entry.value, b"");

        let entry = StaticTable::get(2).unwrap();
        assert_eq!(entry.name, b":method");
        assert_eq!(entry.value, b"GET");

        let entry = StaticTable::get(7).unwrap();
        assert_eq!(entry.name, b":scheme");
        assert_eq!(entry.value, b"https");

        // Test bounds
        assert!(StaticTable::get(0).is_none());
        assert!(StaticTable::get(62).is_none());
        assert!(StaticTable::get(61).is_some());
    }

    #[test]
    fn test_static_table_find() {
        // Exact match
        let (idx, exact) = StaticTable::find(b":method", b"GET").unwrap();
        assert_eq!(idx, 2);
        assert!(exact);

        // Name match only
        let (idx, exact) = StaticTable::find(b":method", b"PUT").unwrap();
        assert_eq!(idx, 2); // First :method entry
        assert!(!exact);

        // No match
        assert!(StaticTable::find(b"x-custom", b"value").is_none());
    }

    #[test]
    fn test_dynamic_table_insert() {
        let mut table = DynamicTable::new(256);

        table.insert(HeaderField::new(
            b"custom-header".to_vec(),
            b"value1".to_vec(),
        ));
        assert_eq!(table.len(), 1);

        table.insert(HeaderField::new(
            b"another-header".to_vec(),
            b"value2".to_vec(),
        ));
        assert_eq!(table.len(), 2);

        // Newest entry should be at index 0
        let entry = table.get(0).unwrap();
        assert_eq!(entry.name, b"another-header");
    }

    #[test]
    fn test_dynamic_table_eviction() {
        // Small table that can only hold a few entries
        let mut table = DynamicTable::new(100);

        // Each entry is ~45 bytes (name + value + 32)
        table.insert(HeaderField::new(b"header1".to_vec(), b"value1".to_vec()));
        table.insert(HeaderField::new(b"header2".to_vec(), b"value2".to_vec()));

        assert_eq!(table.len(), 2);

        // This should cause eviction
        table.insert(HeaderField::new(b"header3".to_vec(), b"value3".to_vec()));

        // Should have evicted the oldest entry
        assert!(table.len() <= 2);
    }

    #[test]
    fn test_dynamic_table_resize() {
        let mut table = DynamicTable::new(256);

        table.insert(HeaderField::new(b"header1".to_vec(), b"value1".to_vec()));
        table.insert(HeaderField::new(b"header2".to_vec(), b"value2".to_vec()));

        // Shrink the table
        table.set_max_size(50);

        // Should have evicted entries
        assert!(table.size() <= 50);
    }

    #[test]
    fn test_header_field_size() {
        let field = HeaderField::new(b"content-type".to_vec(), b"application/json".to_vec());
        // 12 + 16 + 32 = 60
        assert_eq!(field.size(), 60);
    }
}
