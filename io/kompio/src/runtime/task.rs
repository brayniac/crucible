use std::future::Future;
use std::pin::Pin;

type BoxFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;

/// State of a single task slot.
enum TaskSlot {
    /// Slot is empty (no task).
    Empty,
    /// Task is parked (waiting for a wakeup).
    Parked(BoxFuture),
    /// Task is ready to be polled.
    Ready(BoxFuture),
}

/// Slab of per-connection async tasks, indexed by connection index.
///
/// One long-lived task per connection. Spawned on accept/connect,
/// dropped on close. O(1) lookup by `ConnToken::index()`.
pub(crate) struct TaskSlab {
    tasks: Vec<TaskSlot>,
}

impl TaskSlab {
    /// Create a new task slab with capacity for `max_connections` tasks.
    pub(crate) fn new(max_connections: u32) -> Self {
        let mut tasks = Vec::with_capacity(max_connections as usize);
        for _ in 0..max_connections {
            tasks.push(TaskSlot::Empty);
        }
        TaskSlab { tasks }
    }

    /// Spawn a new task for the given connection index.
    /// The task is immediately marked as Ready for its first poll.
    pub(crate) fn spawn(&mut self, conn_index: u32, future: BoxFuture) {
        let idx = conn_index as usize;
        debug_assert!(idx < self.tasks.len(), "conn_index out of range");
        debug_assert!(
            matches!(self.tasks[idx], TaskSlot::Empty),
            "task already exists for conn_index {conn_index}"
        );
        self.tasks[idx] = TaskSlot::Ready(future);
    }

    /// Take a Ready task out for polling. Returns None if the slot is
    /// not in the Ready state.
    pub(crate) fn take_ready(&mut self, conn_index: u32) -> Option<BoxFuture> {
        let idx = conn_index as usize;
        if idx >= self.tasks.len() {
            return None;
        }
        match std::mem::replace(&mut self.tasks[idx], TaskSlot::Empty) {
            TaskSlot::Ready(fut) => Some(fut),
            other => {
                // Put it back — was not Ready.
                self.tasks[idx] = other;
                None
            }
        }
    }

    /// Park a task back after it returned Poll::Pending.
    pub(crate) fn park(&mut self, conn_index: u32, future: BoxFuture) {
        let idx = conn_index as usize;
        debug_assert!(idx < self.tasks.len());
        self.tasks[idx] = TaskSlot::Parked(future);
    }

    /// Mark a Parked task as Ready (called when the waker fires).
    /// Returns true if the task was parked and is now ready.
    pub(crate) fn wake(&mut self, conn_index: u32) -> bool {
        let idx = conn_index as usize;
        if idx >= self.tasks.len() {
            return false;
        }
        match std::mem::replace(&mut self.tasks[idx], TaskSlot::Empty) {
            TaskSlot::Parked(fut) => {
                self.tasks[idx] = TaskSlot::Ready(fut);
                true
            }
            TaskSlot::Ready(fut) => {
                // Already ready — put it back.
                self.tasks[idx] = TaskSlot::Ready(fut);
                false // already queued
            }
            TaskSlot::Empty => false,
        }
    }

    /// Remove a task (connection closed or future completed).
    pub(crate) fn remove(&mut self, conn_index: u32) {
        let idx = conn_index as usize;
        if idx < self.tasks.len() {
            self.tasks[idx] = TaskSlot::Empty;
        }
    }

    /// Check if a task exists for the given connection index.
    #[allow(dead_code)]
    pub(crate) fn has_task(&self, conn_index: u32) -> bool {
        let idx = conn_index as usize;
        idx < self.tasks.len() && !matches!(self.tasks[idx], TaskSlot::Empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll};

    /// A simple future that resolves after being polled N times.
    struct CountdownFuture(u32);

    impl Future for CountdownFuture {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 == 0 {
                Poll::Ready(())
            } else {
                self.0 -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    #[test]
    fn spawn_and_take_ready() {
        let mut slab = TaskSlab::new(4);
        assert!(!slab.has_task(0));

        slab.spawn(0, Box::pin(CountdownFuture(2)));
        assert!(slab.has_task(0));

        // Should be Ready immediately after spawn.
        let fut = slab.take_ready(0);
        assert!(fut.is_some());

        // After taking, slot is Empty.
        assert!(!slab.has_task(0));
    }

    #[test]
    fn park_and_wake() {
        let mut slab = TaskSlab::new(4);
        slab.spawn(1, Box::pin(CountdownFuture(1)));

        let fut = slab.take_ready(1).unwrap();

        // Park the future.
        slab.park(1, fut);
        assert!(slab.has_task(1));

        // Not ready yet.
        assert!(slab.take_ready(1).is_none());

        // Wake it.
        assert!(slab.wake(1));

        // Now it's ready.
        assert!(slab.take_ready(1).is_some());
    }

    #[test]
    fn remove_task() {
        let mut slab = TaskSlab::new(4);
        slab.spawn(2, Box::pin(CountdownFuture(0)));
        assert!(slab.has_task(2));

        slab.remove(2);
        assert!(!slab.has_task(2));
    }

    #[test]
    fn wake_empty_slot() {
        let mut slab = TaskSlab::new(4);
        assert!(!slab.wake(3));
    }

    #[test]
    fn wake_already_ready() {
        let mut slab = TaskSlab::new(4);
        slab.spawn(0, Box::pin(CountdownFuture(0)));

        // Already ready — wake should return false (already queued).
        assert!(!slab.wake(0));
    }
}
