import pika
import uuid
import json

class RpcClient:
    def __init__(self):
        # Menghubungkan ke RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        # Membuat queue sementara untuk menerima respons
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Menerima respons dari server
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)
    
    # Callback untuk menerima respons
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
    
    # Mengirim request RPC
    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        # Mengirimkan request RPC ke server
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id),
            body=str(n)
        )

        # Menunggu respons dari server
        while self.response is None:
            self.connection.process_data_events()
        
        return json.loads(self.response.decode())

def RpcRequest(limit):
    # Mengirim permintaan untuk bilangan prima hingga angka tertentu
    print(f"Mengirim permintaan untuk bilangan prima hingga {limit}...")
    return rpc_client.call(limit)


if __name__ == "__main__":
    # Menghubungkan ke server RPC
    rpc_client = RpcClient()
    
    try:
        while True:
            try:
                inpt = input('Masukkan bilangan yang akan dijadikan limit (ketik "exit" untuk keluar) : ')
                if(inpt == 'exit'):
                    break
                limit = int(inpt)
            except ValueError:
                print('Format anda salah, Harap untuk hanya memasukkan angka')
                continue
            
            # Menjalankan RPC Request ke server
            response = RpcRequest(limit)
            
            # Menampilkan hasil
            print(f"Bilangan prima hingga {limit}: {response}")
    except KeyboardInterrupt:
        print("\nProgram dihentikan oleh pengguna.")
    finally:
        # Menutup koneksi ke server
        rpc_client.connection.close()
        print("Koneksi ditutup.")
