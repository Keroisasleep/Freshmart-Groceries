from kombu import Connection, Exchange, Queue, Consumer

class PayrollService:
    def __init__(self):
        self.salaries = {}
        self.connection = Connection('amqp://guest:guest@localhost//')
        self.exchange = Exchange('employee_exchange', type='direct')
        self.queue = Queue('payroll_queue', exchange=self.exchange, routing_key='employee')

    def process_message(self, body, message):
        event_type = body['event_type']
        data = body['data']
        
        if event_type == 'employee.added':
            self.add_employee_to_payroll(data)
        message.ack()

    def add_employee_to_payroll(self, employee_data):
        employee_id = employee_data['employee_id']
        self.salaries[employee_id] = {'base_salary': 50000}
        print(f"Employee {employee_data['name']} added to payroll with base salary $50000.")

    def listen_for_events(self):
        with self.connection as conn:
            with Consumer(conn, queues=[self.queue], callbacks=[self.process_message], accept=['json']):
                print("Payroll service listening for employee events...")
                while True:
                    conn.drain_events()

if __name__ == '__main__':
    payroll_service = PayrollService()
    payroll_service.listen_for_events()
