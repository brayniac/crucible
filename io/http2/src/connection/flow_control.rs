//! HTTP/2 flow control.

/// Flow control state for connection or stream level.
///
/// HTTP/2 uses a credit-based flow control scheme. Each side maintains
/// a send window that limits how much data can be in flight.
#[derive(Debug)]
pub struct FlowControl {
    /// Current window size.
    window: i32,
    /// Initial window size (for calculating updates).
    initial_window: u32,
    /// Bytes consumed since last window update.
    consumed: u32,
    /// Threshold for sending window updates (fraction of initial window).
    update_threshold: u32,
}

impl FlowControl {
    /// Create new flow control state.
    pub fn new(initial_window_size: u32) -> Self {
        Self {
            window: initial_window_size as i32,
            initial_window: initial_window_size,
            consumed: 0,
            // Send update when we've consumed half the window
            update_threshold: initial_window_size / 2,
        }
    }

    /// Get current available window.
    pub fn available(&self) -> i32 {
        self.window
    }

    /// Increase the window (from WINDOW_UPDATE).
    pub fn increase_window(&mut self, increment: u32) {
        self.window = self.window.saturating_add(increment as i32);
    }

    /// Consume window capacity (data sent or received).
    pub fn consume(&mut self, amount: u32) {
        self.window -= amount as i32;
        self.consumed += amount;
    }

    /// Check if we should send a WINDOW_UPDATE.
    pub fn should_update(&self) -> bool {
        self.consumed >= self.update_threshold
    }

    /// Get the pending window update amount.
    pub fn pending_update(&self) -> u32 {
        self.consumed
    }

    /// Reset consumed counter after sending WINDOW_UPDATE.
    pub fn reset_pending(&mut self) {
        self.window += self.consumed as i32;
        self.consumed = 0;
    }

    /// Adjust window for settings change.
    pub fn adjust_window(&mut self, delta: i32) {
        self.window = self.window.saturating_add(delta);
    }

    /// Get the initial window size.
    pub fn initial_window(&self) -> u32 {
        self.initial_window
    }

    /// Set a new initial window size.
    pub fn set_initial_window(&mut self, new_initial: u32) {
        let delta = new_initial as i32 - self.initial_window as i32;
        self.initial_window = new_initial;
        self.window = self.window.saturating_add(delta);
        self.update_threshold = new_initial / 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let fc = FlowControl::new(65535);
        assert_eq!(fc.available(), 65535);
        assert!(!fc.should_update());
    }

    #[test]
    fn test_consume_and_update() {
        let mut fc = FlowControl::new(65535);

        // Consume some data
        fc.consume(30000);
        assert_eq!(fc.available(), 35535);
        assert!(!fc.should_update()); // Haven't hit threshold yet

        // Consume more to hit threshold
        fc.consume(10000);
        assert_eq!(fc.available(), 25535);
        assert!(fc.should_update()); // Now should update

        // Get pending and reset
        assert_eq!(fc.pending_update(), 40000);
        fc.reset_pending();
        assert_eq!(fc.available(), 65535); // Window restored
        assert!(!fc.should_update());
    }

    #[test]
    fn test_window_increase() {
        let mut fc = FlowControl::new(65535);

        fc.consume(30000);
        assert_eq!(fc.available(), 35535);

        fc.increase_window(20000);
        assert_eq!(fc.available(), 55535);
    }

    #[test]
    fn test_settings_adjustment() {
        let mut fc = FlowControl::new(65535);

        // Consume some data
        fc.consume(10000);
        assert_eq!(fc.available(), 55535);

        // Adjust for new settings (increase by 10000)
        fc.adjust_window(10000);
        assert_eq!(fc.available(), 65535);
    }
}
