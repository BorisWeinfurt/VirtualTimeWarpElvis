use crate::time::input_queue::InputQueue;
use crate::time::message::{MachineId, Message, MessagePayload, Sign, VirtualTime};
use crate::time::output_queue::OutputQueue;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;

// This is the machine struct, it holds the machines state variables as 
// well as the things needed for virtual time. For simplicity its all in
// a single struct but ideally in a finished version there would be some
// additional abstraction between the time and machine and it would be 
// implemented as a trait to allow for more flexibility.

// The main idea behind this system is that each machine has a local virtual
// time. These can be different between machines since they are only local. 
// Machines will have a queue of messages that are being received and it will
// always process the ones with the lowest virtual time first. The machine
// assumes that messages will come in the correct order and process them as they
// come in, when a message inevitably comes out of order the machine will rollback
// to the last state it was in just before the out of order message should have
// been received and continue execution. 
pub struct Machine {
    machine_id: MachineId,
    local_virtual_time: VirtualTime,
    pub state: MachineState,
    pub input_queue: InputQueue,
    pub output_queue: OutputQueue,
    state_queue: BTreeSet<StampedMachineState>,
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct MachineState {
    local_var1: String,
    local_var2: i32,
}
impl MachineState {
    pub fn new() -> Self {
        Self::default()
    }
}

// Wrapper to allow sorted order of machine states
#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct StampedMachineState {
    machine_state: Option<MachineState>,
    virtual_time_stamp: VirtualTime,
}

impl Ord for StampedMachineState {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .virtual_time_stamp
            .cmp(&self.virtual_time_stamp)
            .reverse()
    }
}

impl PartialOrd for StampedMachineState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Machine {
    // In an actual imlementation virtual time could either be assigned by a global management system
    // or just initialized to 0 for all machines
    pub fn new(machine_id: MachineId, local_virtual_time: VirtualTime) -> Self {
        let mut self_var = Self {
            machine_id,
            local_virtual_time,
            input_queue: InputQueue::new(local_virtual_time),
            output_queue: OutputQueue::new(),
            state: MachineState::new(),
            state_queue: BTreeSet::new(),
        };
        self_var.state_queue.insert(StampedMachineState {
            virtual_time_stamp: 0,
            machine_state: Some(MachineState::new()),
        });
        return self_var;
    }
    // This function receives the messages and puts them in the input queue so
    // that they are ready to be processed by the inner function. If a message is
    // received with a lower receive time than self.virtualtime then we have missed 
    // the point in virtual time this message should have been received an rollback
    pub fn recieve_outer(&mut self, message: Message) -> Option<Vec<Message>> {
        if message.rec_time >= self.local_virtual_time {
            self.input_queue.insert(message);
            return None;
        } else {
            // Rollback:
            // 1: find the most recent correct state and restore it
            // 2: discard unnecessary states
            // 3: "unsend" messages that have already been sent
            // 4: update the local_virtual_time
            // 5: insert the message

            // 1
            let rollback_target = message.rec_time;
            let threshold = StampedMachineState {
                machine_state: None,
                virtual_time_stamp: rollback_target,
            };
            let most_recent_state = self
                .state_queue
                .range((
                    Included(&StampedMachineState {
                        machine_state: None,
                        virtual_time_stamp: 0,
                    }),
                    Excluded(&threshold),
                ))
                .next()
                .unwrap()
                .clone();
            self.state = most_recent_state.machine_state.unwrap();
            // 2
            let states_to_delete: Vec<_> = self
                .state_queue
                .range(
                    StampedMachineState {
                        virtual_time_stamp: rollback_target,
                        machine_state: None,
                    }..=StampedMachineState {
                        virtual_time_stamp: self.local_virtual_time,
                        machine_state: None,
                    },
                )
                .cloned()
                .collect();
            for state in states_to_delete {
                self.state_queue.remove(&state);
            }
            // 3
            let sent_antimessages: Vec<_> = self
                .output_queue
                .range(rollback_target, self.local_virtual_time).iter()
                .map(|message| {
                    // Create a new message with the sign modified to Antimessage
                    let mut modified_message = message.clone();
                    modified_message.sign = Sign::Antimessage;

                    // Send the modified message
                    self.send_outer(modified_message)
                })
                .collect();

            // 4
            self.local_virtual_time = rollback_target;
            // offset by one to allow next call of get_next_message to get this message
            self.input_queue.update_threshold(rollback_target - 1);

            // 5
            self.input_queue.insert(message);

            return Some(sent_antimessages);
        }
    }

    // Helper function to get a function from the input queue while updating the necessary variables
    fn get_next_message(&mut self) -> Option<Message> {
        let message = self.input_queue.peek_smallest_greater().unwrap();
        if message.sign == Sign::Antimessage {
            return None;
        }
        self.state_queue.insert(StampedMachineState {
            machine_state: Some(self.state.clone()),
            virtual_time_stamp: self.local_virtual_time,
        });
        // sanity check
        if self.local_virtual_time > message.rec_time {
            panic!("Messages in input queue should always be valid");
        }
        self.local_virtual_time = message.rec_time;
        self.input_queue.update_threshold(message.rec_time);

        Some(message)
    }
    // This is where the machine actually operates on the messages its receiving and
    // executes any logic that it wants to

    // Normally this could be private since the machine would just process
    // messages from its queues whenever, for demonstration its public for manual control
    pub fn recieve_inner(&mut self) {
        let message = match self.get_next_message() {
            Some(msg) => msg,
            None => {
                println!("Found an antimessage, skipping processing since it would guarentee a rollback");
                return;
            },
        };
        
        println!("Received message : {:?}", message);

        self.state.local_var2 += 5;
        println!(
            "Adding 5 to current state, now at : {}",
            self.state.local_var2
        )
    }

    // Very simple helper similar to receive outer except sending a message cant
    // cause a rollback. Depending on implementation the message wrapper may be undesirable
    // in which case the outer functions could handle that as well.
    pub fn send_outer(&mut self, message: Message) -> Message {
        self.output_queue.push(message.clone());
        message
    }

    // This is where the machine can create/send its own messages, maybe upon reaching some state or in
    // respons to some message that was received.

    // Normally this could be private since the machine would have logic inside where it would decide to send
    // For the purpose of demonstration this is public and called from outside
    pub fn send_inner(&mut self) -> Message {
        // Logic to create a message payload goes here a default is used for now
        let my_message = "example message".to_string();

        let wrapped_message = self.make_message(my_message, Sign::Message);
        self.send_outer(wrapped_message)
    }

    // Helper function to make messages that should be delivered in 5 virtual time units from now
    // to an arbitrary machine 0, this may be removed in a real implementation but helps for now
    fn make_message(&self, message: MessagePayload, sign: Sign) -> Message {
        Message {
            send_time: self.local_virtual_time,
            rec_time: self.local_virtual_time + 5,
            sender: self.machine_id,
            receiver: 0,
            sign,
            message: Arc::new(message),
        }
    }
}
