use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;

use super::message::{self, Message};
// Wrapper for sorting by send_time
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MessageBySendTime(pub Message);

impl Ord for MessageBySendTime {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.send_time.cmp(&other.0.send_time)
    }
}

impl PartialOrd for MessageBySendTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// The output queue is just a priority queue of sent messages but is still
// a little special because at times we need to access elemens that are not the lowest
// priority element. This is where the range function comes in. Also like the input_queue
// duplicates are always eliminated to support the message/antimessage system.
#[derive(Debug, Default)]
pub struct OutputQueue {
    set: BTreeSet<MessageBySendTime>,
}

impl OutputQueue {
    pub fn new() -> Self {
        Self {
            set: BTreeSet::new(),
        }
    }

    pub fn push(&mut self, message: Message) {
        let wrapped_message = MessageBySendTime(message);
        if self.set.contains(&wrapped_message) {
            self.set.remove(&wrapped_message);
        } else {
            self.set.insert(wrapped_message);
        }
    }

    pub fn pop(&mut self) -> Option<Message> {
        if let Some(first) = self.set.iter().next().cloned() {
            self.set.remove(&first);
            Some(first.0)
        } else {
            None
        }
    }

    // Get all the messages within a range, does not remove the elements
    pub fn range(&self, start: usize, end: usize) -> Vec<Message> {
        let start = MessageBySendTime(Message {
            send_time: start,
            rec_time: 0,
            receiver: 0,
            sender: 0,
            sign: super::message::Sign::Message,
            message: Arc::new(String::new()),
        });
        let end = MessageBySendTime(Message {
            send_time: end,
            rec_time: 0,
            receiver: 0,
            sender: 0,
            sign: super::message::Sign::Message,
            message: Arc::new(String::new()),
        });

        self.set
            .range(&start..=&end)
            .cloned()
            .map(|element| element.0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::message::Sign;
    use std::sync::Arc;

    #[test]
    fn test_push_pop_send_time() {
        let msg1 = Message {
            send_time: 1,
            rec_time: 0,
            sender: 0,
            receiver: 0,
            sign: Sign::Message,
            message: Arc::new("Test".to_string()),
        };

        let msg2 = Message {
            send_time: 2,
            rec_time: 0,
            sender: 0,
            receiver: 0,
            sign: Sign::Message,
            message: Arc::new("MessagePayload".to_string()),
        };

        let msg3 = Message {
            send_time: 3,
            rec_time: 0,
            sender: 0,
            receiver: 0,
            sign: Sign::Message,
            message: Arc::new("MessagePayload".to_string()),
        };

        let mut pq = OutputQueue::new();
        pq.push(msg1.clone());
        pq.push(msg2.clone());
        pq.push(msg3.clone());

        println!("{:?}", pq.set);
        assert_eq!(pq.pop(), Some(msg1.clone()));
        assert_eq!(pq.pop(), Some(msg2.clone()));
        assert_eq!(pq.pop(), Some(msg3.clone()));
        assert_eq!(pq.pop(), None);

        
        pq.push(msg3.clone());
        pq.push(msg2.clone());
        pq.push(msg1.clone());

        assert_eq!(pq.pop(), Some(msg1));
        assert_eq!(pq.pop(), Some(msg2));
        assert_eq!(pq.pop(), Some(msg3));
        assert_eq!(pq.pop(), None);
    }

    #[test]
    fn test_push_duplicate() {
        let msg1 = Message {
            send_time: 1,
            rec_time: 5,
            sender: 1,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("MessagePayload".to_string()),
        };
        assert_eq!(msg1, msg1);

        let mut pq = OutputQueue::new();
        pq.push(msg1.clone());
        pq.push(msg1.clone());

        assert_eq!(pq.pop(), None);
    }

    #[test]
    fn test_range() {
        let msg1 = Message {
            send_time: 1,
            rec_time: 0,
            sender: 0,
            receiver: 0,
            sign: Sign::Message,
            message: Arc::new("Test".to_string()),
        };

        let msg2 = Message {
            send_time: 2,
            rec_time: 0,
            sender: 0,
            receiver: 0,
            sign: Sign::Message,
            message: Arc::new("MessagePayload".to_string()),
        };

        let msg3 = Message {
            send_time: 3,
            rec_time: 0,
            sender: 0,
            receiver: 0,
            sign: Sign::Message,
            message: Arc::new("MessagePayload".to_string()),
        };

        let mut pq = OutputQueue::new();
        pq.push(msg1.clone());
        pq.push(msg2.clone());
        pq.push(msg3.clone());

        let range_result = pq.range(1, 3);
        assert_eq!(range_result, vec![msg1.clone(), msg2.clone(), msg3.clone()]);

        let range_result = pq.range(2, 4);
        assert_eq!(range_result, vec![msg2.clone(), msg3.clone()]);

        let range_result = pq.range(1, 4);
        assert_eq!(range_result, vec![msg1.clone(), msg2.clone(), msg3.clone()]);

        let range_result = pq.range(4, 5);
        assert_eq!(range_result, vec![]);
    }

}
