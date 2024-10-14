import pika
import math
import json

# Fungsi untuk mengecek bilangan prima
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

# Fungsi RPC yang mengembalikan daftar bilangan prima
def rpc_prima(limit):
    primes = [i for i in range(2, limit + 1) if is_prime(i)]
    return primes

# Menghubungkan ke RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Membuat queue RPC
channel.queue_declare(queue='rpc_queue')

# Fungsi callback untuk memproses request dari client
def on_request(ch, method, props, body):
    limit = int(body.decode())
    
    print(f"Server menerima permintaan batas {limit}")

    # Memproses permintaan untuk mengembalikan daftar bilangan prima
    response = rpc_prima(limit)
    
    # Mengirimkan respons ke client
    ch.basic_publish(exchange='',
                    routing_key=props.reply_to,
                    properties=pika.BasicProperties(correlation_id=props.correlation_id),
                    body=json.dumps(response),
                    )
    
    # Menandai pesan sebagai selesai diproses
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Konsumsi pesan dari queue 'rpc_queue'
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

try:
    # Memulai menerima permintaan
    print("Server RPC menunggu permintaan... (Gunakan ctrl+c untuk menutup)")
    channel.start_consuming()
except KeyboardInterrupt:
    # Menutup koneksi ketika ctrl+c ditekan
    print("Server dihentikan oleh pengguna.")
    channel.stop_consuming()
    connection.close()
