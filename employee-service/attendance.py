from kombu import Connection, Exchange, Queue, Consumer

class AttendanceService:
    def __init__(self):
        self.attendance_records = {}
        self.connection = Connection('amqp://guest:guest@localhost//')
        self.exchange = Exchange('employee_exchange', type='direct')
        self.queue = Queue('attendance_queue', exchange=self.exchange, routing_key='employee')

    def process_message(self, body, message):
        event_type = body['event_type']
        data = body['data']
        
        if event_type == 'employee.added':
            self.create_attendance_record(data)
        message.ack()

    def create_attendance_record(self, employee_data):
        employee_id = employee_data['employee_id']
        self.attendance_records[employee_id] = {'days_present': 0, 'days_absent': 0}
        print(f"Attendance record created for employee {employee_data['name']}.")

    def listen_for_events(self):
        with self.connection as conn:
            with Consumer(conn, queues=[self.queue], callbacks=[self.process_message], accept=['json']):
                print("Attendance service listening for employee events...")
                while True:
                    conn.drain_events()

if __name__ == '__main__':
    attendance_service = AttendanceService()
    attendance_service.listen_for_events()
