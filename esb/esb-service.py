from kombu import Exchange, Queue, Connection, Consumer, Producer

class ESB:
    def __init__(self):
        # Connection to the message broker (RabbitMQ)
        self.connection = Connection('amqp://guest:guest@localhost//')
        
        # Define an exchange for broadcasting employee-related messages
        self.exchange = Exchange('employee_exchange', type='direct')

        # Define queues for services (Employee, Payroll, Attendance)
        self.employee_queue = Queue('employee_queue', exchange=self.exchange, routing_key='employee')
        self.payroll_queue = Queue('payroll_queue', exchange=self.exchange, routing_key='employee')
        self.attendance_queue = Queue('attendance_queue', exchange=self.exchange, routing_key='employee')

    def start(self):
        print("ESB started and routing messages...")
        
        with self.connection as conn:
            # This ensures the exchange and queues are declared (created) in RabbitMQ
            producer = Producer(conn)
            producer.declare(self.exchange)
            producer.declare(self.employee_queue)
            producer.declare(self.payroll_queue)
            producer.declare(self.attendance_queue)

            # Start consumers for all queues, each service will subscribe to its queue
            self.listen_for_employee_events(conn)
            self.listen_for_payroll_events(conn)
            self.listen_for_attendance_events(conn)

    def process_message(self, body, message, queue_name):
        event_type = body['event_type']
        data = body['data']
        
        print(f"ESB received message on {queue_name}: {event_type} -> {data}")
        
        # Forward messages as required to other queues (Payroll or Attendance service)
        if event_type == 'employee.added':
            # Forward to payroll and attendance services
            self.forward_message('payroll_queue', body)
            self.forward_message('attendance_queue', body)

        # Acknowledge the message after processing
        message.ack()

    def forward_message(self, queue_name, message):
        with self.connection as conn:
            producer = Producer(conn)
            producer.publish(
                message,
                exchange=self.exchange,
                routing_key=queue_name,
                declare=[Queue(queue_name, exchange=self.exchange, routing_key='employee')]
            )
            print(f"Message forwarded to {queue_name}: {message}")

    def listen_for_employee_events(self, conn):
        # This listens to the employee_queue for new events related to employee actions
        with Consumer(conn, queues=[self.employee_queue], callbacks=[self.process_employee_event], accept=['json']):
            print("Listening for Employee service events...")
            while True:
                conn.drain_events()

    def process_employee_event(self, body, message):
        self.process_message(body, message, "employee_queue")

    def listen_for_payroll_events(self, conn):
        # This listens to the payroll_queue for payroll service events (if needed)
        with Consumer(conn, queues=[self.payroll_queue], callbacks=[self.process_payroll_event], accept=['json']):
            print("Listening for Payroll service events...")
            while True:
                conn.drain_events()

    def process_payroll_event(self, body, message):
        self.process_message(body, message, "payroll_queue")

    def listen_for_attendance_events(self, conn):
        # This listens to the attendance_queue for attendance service events (if needed)
        with Consumer(conn, queues=[self.attendance_queue], callbacks=[self.process_attendance_event], accept=['json']):
            print("Listening for Attendance service events...")
            while True:
                conn.drain_events()

    def process_attendance_event(self, body, message):
        self.process_message(body, message, "attendance_queue")


if __name__ == '__main__':
    esb = ESB()
    esb.start()
