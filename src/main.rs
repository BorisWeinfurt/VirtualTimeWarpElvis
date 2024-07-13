use std::sync::Arc;

use machine::Machine;
use time::message::{Message, Sign};

mod machine;
mod time;


fn main() {
    send_antimessage_first();
}

// Example where a single machine receives 2 messages, they need not be in order. When the
// message is received it is first put into the input queue by the outer function and then 
// the machine can process them at whatever pace it wants using the inner function. This function
// assumes that all the messages happen to come in the correct order, the idea is that this assumption
// should hold most of the time although the system is capable of handling it when it isnt the case
fn simple_message() {
    let mut machine1 = Machine::new(1, 0);

    let message1 = Message::new(0, 3, 0, 1, Sign::Message, Arc::new("message".to_string()));
    let message2 = Message::new(0, 5, 0, 1, Sign::Message, Arc::new("message".to_string()));

    machine1.recieve_outer(message1);
    machine1.recieve_outer(message2);
    
    println!("{:?}", machine1.input_queue);

    machine1.recieve_inner();

    println!("{:?}", machine1.input_queue);
}

// This demonstrates a machine rewinding its own state after it realizes that it has done processed 
// messages that are supposed to come after the one it just received. In this case message2 is processed
// first even though it should have been processed second. Then when message1 comes in the machine rollback
// to the state it was in just before it processed the wrong message.
fn simple_rollback() {
    let mut machine1 = Machine::new(1, 0);

    let message1 = Message::new(0, 3, 0, 1, Sign::Message, Arc::new("message".to_string()));
    let message2 = Message::new(0, 5, 0, 1, Sign::Message, Arc::new("message".to_string()));

    machine1.recieve_outer(message2);
    
    println!("{:?}", machine1.input_queue);
    machine1.recieve_inner();
    
    println!("{:?}", machine1.input_queue);
    machine1.recieve_outer(message1);
    println!("\nPost Rollback:\n\n");
    println!("{:?}", machine1.input_queue);
    
    machine1.recieve_inner();
    machine1.recieve_inner();
}

// This example is very similar to the last example however it rollsback multiple messages in a single pass
// and once it processes them it happens to rollback a second time.
fn extended_rollback() {
    let mut machine1 = Machine::new(1, 0);

    let message1 = Message::new(0, 3, 0, 1, Sign::Message, Arc::new("message".to_string()));
    let message2 = Message::new(0, 4, 0, 1, Sign::Message, Arc::new("message".to_string()));
    let message3 = Message::new(0, 5, 0, 1, Sign::Message, Arc::new("message".to_string()));
    let message4 = Message::new(0, 6, 0, 1, Sign::Message, Arc::new("message".to_string()));
    let message5 = Message::new(0, 7, 0, 1, Sign::Message, Arc::new("message".to_string()));

    // Receive and process messages 3-5
    machine1.recieve_outer(message3);
    machine1.recieve_outer(message4);
    machine1.recieve_outer(message5);

    machine1.recieve_inner();
    machine1.recieve_inner();
    machine1.recieve_inner();
    
    
    println!("Pre-rollback:\n{:?}", machine1.input_queue);

    machine1.recieve_outer(message2);

    println!("Rollback 1:\n{:?}", machine1.input_queue);

    machine1.recieve_inner();
    machine1.recieve_inner();
    machine1.recieve_inner();
    machine1.recieve_inner();

    println!("Pre-second-rollback:\n{:?}", machine1.input_queue);
    
    machine1.recieve_outer(message1);

    println!("Rollback 2:\n{:?}", machine1.input_queue);

    machine1.recieve_inner();
    machine1.recieve_inner();
    machine1.recieve_inner();
    machine1.recieve_inner();
    machine1.recieve_inner();
}

// This is an example of sending a message, this example doesnt implement channels or any kind of message
// passing between machines so send just returns the message which is then passed in manually. The send is fairly
// straighforward, the machine does the work it wants to do in send and wraps the payload in the message struct.
// Then the machine logs that it sent the message for later.
fn send_message() {
    let mut machine1 = Machine::new(1, 0);
    let mut machine2 = Machine::new(2, 0);

    let message1 = Message::new(0, 3, 1, 2, Sign::Message, Arc::new("message".to_string()));
    let message2 = Message::new(2, 5, 1, 2, Sign::Message, Arc::new("message".to_string()));

    let sent_message1 = machine1.send_outer(message1);
    machine2.recieve_outer(sent_message1);
    
    let sent_message2 = machine1.send_outer(message2);
    machine2.recieve_outer(sent_message2);
    
    println!("Machine 1 output queue: {:?}", machine1.output_queue);
    println!("Machine 2 input queue: {:?}", machine2.input_queue);

    machine2.recieve_inner();
    machine2.recieve_inner();

    println!("{:?}", machine1.input_queue);
}

// During some period of time a machine not only alters its own state but also sends messages to other machines. Then
// when rolling back you need to revert not only the state but also chase down the messages that you sent to other machines
// to revert them. This is done by sending "antimessage" that cancel out the original message that was sent. 
// There are 3 cases in which this can happen which are shown below.

// When M1 sends to M2 the messages arrive in M2's input queue but it may or may not have processed them yet. In this case
// it has not been processed so when M1 rollsback it needs to cancel out the messages it sent to M2. This is done by sending
// antimessages, and since M2 hasnt processed the reqular messages yet they are just cancel out and M2 never even realizes
// that it received messages in the wrong order.
fn send_message_simple_rollback() {
    let mut machine1 = Machine::new(1, 0);
    let mut machine2 = Machine::new(2, 0);

    let message0 = Message::new(1, 3, 2, 1, Sign::Message, Arc::new("message1".to_string()));
    let message1 = Message::new(1, 3, 1, 2, Sign::Message, Arc::new("message2".to_string()));
    let message2 = Message::new(2, 5, 1, 2, Sign::Message, Arc::new("message3".to_string()));
    let message3 = Message::new(0, 1, 1, 2, Sign::Message, Arc::new("message4".to_string()));

    machine1.recieve_outer(message0);
    machine1.recieve_inner();

    let sent_message1 = machine1.send_outer(message1);
    machine2.recieve_outer(sent_message1);
    
    let sent_message2 = machine1.send_outer(message2);
    machine2.recieve_outer(sent_message2);
    
    println!("Machine 1 output queue: {:?}\n", machine1.output_queue);
    println!("Machine 1 input queue: {:?}\n", machine1.input_queue);
    println!("Machine 2 input queue: {:?}\n", machine2.input_queue);

    // ROLLBACK HAPPENING
    let messages_to_unsend = machine1.recieve_outer(message3).unwrap();
    for message in messages_to_unsend {
        let _ = machine2.recieve_outer(message.clone());
    }

    println!("\n\n\nPost rollback!\n\n");
    println!("Machine 1 output queue: {:?}\n", machine1.output_queue);
    println!("Machine 1 input queue: {:?}\n", machine1.input_queue);
    println!("Machine 2 input queue: {:?}\n", machine2.input_queue);

}

// This is a more complex example where M1 sends to M2 and M2 ends up processing the messages before M1 rollsback. In this case
// everything happens the same as the last case up until the antimessages are received by M2. Because M2 has already processed the
// regular messages we know that its local virtual time is already greater than that message, so when the antimessage comes M2
// know it is receiving something out of order and itself rollsback to just before it received the out of order method.
fn send_message_double_rollback() {
    let mut machine1 = Machine::new(1, 0);
    let mut machine2 = Machine::new(2, 0);

    let message0 = Message::new(1, 3, 2, 1, Sign::Message, Arc::new("message1".to_string()));
    let message1 = Message::new(1, 3, 1, 2, Sign::Message, Arc::new("message2".to_string()));
    let message2 = Message::new(2, 5, 1, 2, Sign::Message, Arc::new("message3".to_string()));
    let message3 = Message::new(0, 1, 1, 2, Sign::Message, Arc::new("message4".to_string()));

    machine1.recieve_outer(message0);
    machine1.recieve_inner();

    let sent_message1 = machine1.send_outer(message1);
    machine2.recieve_outer(sent_message1);
    
    let sent_message2 = machine1.send_outer(message2);
    machine2.recieve_outer(sent_message2);
    
    machine2.recieve_inner();
    machine2.recieve_inner();

    println!("Machine 1 output queue: {:?}\n", machine1.output_queue);
    println!("Machine 1 input queue: {:?}\n", machine1.input_queue);
    println!("Machine 2 input queue: {:?}\n", machine2.input_queue);
    println!("Machine 2 state: {:?}\n", machine2.state);

    // ROLLBACK HAPPENING
    let messages_to_unsend = machine1.recieve_outer(message3).unwrap();
    for message in messages_to_unsend {
        let _ = machine2.recieve_outer(message.clone());
    }

    println!("\n\n\nPost rollback!\n\n");
    println!("Machine 1 output queue: {:?}\n", machine1.output_queue);
    println!("Machine 1 input queue: {:?}\n", machine1.input_queue);
    println!("Machine 2 input queue: {:?}\n", machine2.input_queue);
    println!("Machine 2 state: {:?}\n", machine2.state);

}

// The last case may be rare (or not existant depending on implementation) but still works under this system. In an asynchronous
// setting if M1 sends a message then rollsback and sends an antimessage it is possible in some cases that the antimessage actually
// arrives before the regular message. In this case if the antimessage is processed as normal and increments the local virtual time
// the arrival of the regular message later will cause the rollback. There is an optimization here because if an antimessage is ever
// at the front of the queue you can do a no-op and wait until it cancels out because processing it guarentees a rollback.
fn send_antimessage_first() {
    let mut machine1 = Machine::new(1, 0);
    let mut machine2 = Machine::new(2, 0);

    let message0 = Message::new(1, 3, 2, 1, Sign::Message, Arc::new("message1".to_string()));
    let message1 = Message::new(1, 3, 1, 2, Sign::Message, Arc::new("message2".to_string()));
    let message2 = Message::new(2, 5, 1, 2, Sign::Message, Arc::new("message3".to_string()));
    let message3 = Message::new(0, 1, 1, 2, Sign::Message, Arc::new("message4".to_string()));

    machine1.recieve_outer(message0);
    machine1.recieve_inner();

    let sent_message1 = machine1.send_outer(message1);
    
    println!("Machine 1 output queue: {:?}\n", machine1.output_queue);
    println!("Machine 1 input queue: {:?}\n", machine1.input_queue);
    println!("Machine 2 input queue: {:?}\n", machine2.input_queue);
    
    // ROLLBACK HAPPENING
    let messages_to_unsend = machine1.recieve_outer(message3).unwrap();
    for message in messages_to_unsend {
        let _ = machine2.recieve_outer(message.clone());
    }
    // Antimessage first in queue so it wont poll
    machine2.recieve_inner();
    // Receive actual message second
    machine2.recieve_outer(sent_message1);
    
    println!("\n\n\nPost rollback!\n\n");
    println!("Machine 1 output queue: {:?}\n", machine1.output_queue);
    println!("Machine 1 input queue: {:?}\n", machine1.input_queue);
    println!("Machine 2 input queue: {:?}\n", machine2.input_queue);

}
