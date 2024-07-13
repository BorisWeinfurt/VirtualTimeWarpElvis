
use std::sync::Arc;

pub type MachineId = usize;
pub type VirtualTime = usize;
pub type MessagePayload = String;

// This is just wrapper around a payload that is being sent.
#[derive(Debug, Clone, Hash)]
pub struct Message {
    pub send_time : VirtualTime,
    pub rec_time : VirtualTime,
    pub sender : MachineId,
    pub receiver : MachineId,
    pub sign : Sign,
    pub message : Arc<MessagePayload>,
}
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Sign {
    Message,
    Antimessage,
}

impl Message {
    pub fn new(
        send_time: VirtualTime,
        rec_time: VirtualTime,
        sender: MachineId,
        receiver: MachineId,
        sign: Sign,
        message: Arc<MessagePayload>,
    ) -> Self {
        Self {
            send_time,
            rec_time,
            sender,
            receiver,
            sign,
            message,
        }
    }
}
// Messages with opposite signs are equivalent
// This is because they should be treated as duplicates and 
// eliminated from the queues they are in.
impl Eq for Message {}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.send_time == other.send_time &&
        self.rec_time == other.rec_time &&
        self.sender == other.sender &&
        self.receiver == other.receiver &&
        Arc::ptr_eq(&self.message, &other.message)
    }
}

