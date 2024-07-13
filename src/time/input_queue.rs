use super::message::Message;
use std::fmt;
use std::cmp::Ordering;
use std::{collections::BTreeMap, ops::Bound, sync::Arc};
//
// This is the queue of messages that are arriving to be processed by a machine. 
// It is essentialy a priority queue with a pointer to some element in the queue. 
// Elements are inserted according to their priority but on duplicate messages
// both the original and new element are deleted to support message/antimessages. When polling 
// instead of returning and removing the element with the highest priority you just return the highest
// priority above a certain threshold. Removing elements is done as a separate operation.

// The purpose is to keep messages you have processed until you know you dont need
// them anymore but you still want to read more messages to continue processing 
// so you need to keep track of where you are currently in the queue.
pub struct InputQueue {
    map: BTreeMap<WrappedMessage, ()>,
    threshold: usize,
}

#[derive(Debug, PartialEq, Eq, Clone)]
// Wrapper exists to have a custom ordering of messages since input is based on rec_time
// and output is based off of send_time
struct WrappedMessage(Message);
impl WrappedMessage {
    fn new(message: Message) -> Self {
        WrappedMessage(message)
    }
}

impl Ord for WrappedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.rec_time.cmp(&other.0.rec_time)
    }
}

impl PartialOrd for WrappedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for InputQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InputQueue")
            .field("threshold", &self.threshold)
            .field("map", &self.map)
            .finish()
    }
}

impl InputQueue {
    pub fn new(threshold: usize) -> Self {
        InputQueue {
            map: BTreeMap::new(),
            threshold,
        }
    }

    // Inserts into the queue, duplicates are eliminated from queue
    pub fn insert(&mut self, message: Message) {
        let wrapped_message = WrappedMessage(message);
        if self.map.contains_key(&wrapped_message) {
            self.map.remove(&wrapped_message);
        } else {
            self.map.insert(wrapped_message, ());
        }
    }

    // Removes the smallest (highest priority) element, this is for purely for
    // Freeing up messages that have no chance of every being rolled back to
    pub fn remove_smallest(&mut self) -> Option<Message> {
        let smallest = self.map.keys().next().cloned();
        if let Some(ref removed) = &smallest {
            self.map.remove(removed);
        }
        smallest.map(|wrapped| wrapped.0)
    }

    // Remove the smallest element greater than the threshold, this
    // will end up being the next message that should be processed by the 
    // machine ie greater than the local time of the machine 
    pub fn peek_smallest_greater(&mut self) -> Option<Message> {
        let smallest_g = self
            .map
            .range((
                Bound::Excluded(&WrappedMessage(Message::new(
                    0,
                    self.threshold,
                    0,
                    0,
                    super::message::Sign::Message,
                    Arc::new("data".to_string()),
                ))),
                Bound::Unbounded,
            ))
            .next()
            .map(|(msg, _)| msg.clone());
        smallest_g.map(|wrapped| wrapped.0)
    }

    // Machine needs to reset its pointer when rolling back
    pub fn update_threshold(&mut self, new_thresh : usize) {
        self.threshold = new_thresh;
    }

    // Print the priority queue (for debugging)
    fn print(&self) {
        for wrapped_message in self.map.keys() {
            println!("{:?}", wrapped_message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::message::Sign;

    use std::sync::Arc;

    #[test]
    fn test_priority_queue_operations() {
        let mut priority_queue = InputQueue::new(5);

        let message1 = Message {
            send_time: 1,
            rec_time: 10,
            sender: 1,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("Hello".to_string()),
        };

        let message2 = Message {
            send_time: 2,
            rec_time: 5,
            sender: 2,
            receiver: 1,
            sign: Sign::Message,
            message: Arc::new("World".to_string()),
        };

        let message3 = Message {
            send_time: 3,
            rec_time: 8,
            sender: 3,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("!".to_string()),
        };

        priority_queue.insert(message1.clone());
        priority_queue.insert(message2.clone());
        priority_queue.insert(message3.clone());

        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message2.clone())
        );
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message3.clone())
        );
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message1.clone())
        );

        assert_eq!(priority_queue.remove_smallest(), None);
    }

    #[test]
    fn test_priority_queue_with_duplicates() {
        let mut priority_queue = InputQueue::new(5);

        // Define messages as variables
        let mut message1 = Message {
            send_time: 2,
            rec_time: 5,
            sender: 1,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("Duplicate".to_string()),
        };

        priority_queue.insert(message1.clone());
        priority_queue.insert(message1.clone());

 
        assert_eq!(priority_queue.remove_smallest(), None);

        priority_queue.insert(message1.clone());
        message1.sign = Sign::Antimessage;
        priority_queue.insert(message1.clone());

 
        assert_eq!(priority_queue.remove_smallest(), None);
    }

    #[test]
    fn test_priority_queue_edge_cases() {
        let mut priority_queue = InputQueue::new(5);

        let message1 = Message {
            send_time: 1,
            rec_time: 7,
            sender: 1,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("Edge".to_string()),
        };

        let message2 = Message {
            send_time: 2,
            rec_time: 3,
            sender: 2,
            receiver: 1,
            sign: Sign::Message,
            message: Arc::new("Cases".to_string()),
        };

        let message3 = Message {
            send_time: 3,
            rec_time: 8,
            sender: 3,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("Testing".to_string()),
        };

        let message4 = Message {
            send_time: 4,
            rec_time: 4,
            sender: 4,
            receiver: 1,
            sign: Sign::Message,
            message: Arc::new("More".to_string()),
        };
        
        let message5 = Message {
            send_time: 5,
            rec_time: 6,
            sender: 5,
            receiver: 2,
            sign: Sign::Message,
            message: Arc::new("Tests".to_string()),
        };

        priority_queue.insert(message1.clone());
        priority_queue.insert(message2.clone());
        priority_queue.insert(message3.clone());

        // Remove smallest element >= threshold
        assert_eq!(
            priority_queue.peek_smallest_greater(),
            Some(message1.clone())
        );
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message2)
        );

        // Insert messages again
        priority_queue.insert(message4.clone());
        priority_queue.insert(message5.clone());

        // Remove smallest element
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message4)
        );
        assert_eq!(
            priority_queue.peek_smallest_greater(),
            Some(message5.clone())
        );
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message5)
        );
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message1)
        );
        assert_eq!(
            priority_queue.remove_smallest(),
            Some(message3)
        );
        assert_eq!(priority_queue.remove_smallest(), None);
    }
}
