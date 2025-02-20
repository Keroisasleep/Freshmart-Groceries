  from kombu import Connection, Exchange, Queue, Producer

class EmployeeService:
    def __init__(self):
        self.employees = {}  # Store employees in memory
        self.connection = Connection('amqp://guest:guest@localhost//')  # RabbitMQ connection
        self.exchange = Exchange('employee_exchange', type='direct')
        self.queue = Queue('employee_queue', exchange=self.exchange, routing_key='employee')
    
    def add_employee(self, employee_id, name, position):
        employee_data = {
            'employee_id': employee_id,
            'name': name,
            'position': position,
        }
        self.employees[employee_id] = employee_data
        
        # Send event to the ESB
        self.send_message('employee.added', employee_data)
        print(f"Employee {name} added successfully!")

    def send_message(self, event_type, payload):
        with self.connection as conn:
            producer = Producer(conn)
            producer.publish(
                {'event_type': event_type, 'data': payload},
                exchange=self.exchange,
                routing_key='employee',
                declare=[self.queue]
            )

if __name__ == '__main__':
    employee_service = EmployeeService()
    employee_service.add_employee(1, "John Doe", "Software Engineer")
