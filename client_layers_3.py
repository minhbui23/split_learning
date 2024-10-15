import pika
import pickle
import argparse

import torch
import torch.nn as nn
import torch.optim as optim

from src.RpcClient import RpcClient
from src.Model import ModelPart3

parser = argparse.ArgumentParser(description="Split learning framework")
parser.add_argument('--id', type=int, required=True, help='ID of client')

args = parser.parse_args()
assert args.id is not None, "Must provide id for client."

layer_id = 3
client_id = args.id
address = "192.168.101.234"
username = "dai"
password = "dai"

device = None

if torch.cuda.is_available():
    device = "cuda"
    print(f"Using device: {torch.cuda.get_device_name(device)}")
else:
    device = "cpu"
    print(f"Using device: CPU")


model = ModelPart3()
optimizer = optim.SGD(model.parameters(), lr=0.01)
criterion = nn.CrossEntropyLoss()


credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(pika.ConnectionParameters(address, 5672, '/', credentials))


def send_gradient(data_id, gradient, trace):
    channel = connection.channel()
    to_client_id = trace[-1]
    trace.pop(-1)
    backward_queue_name = f'gradient_queue_{layer_id - 1}_{to_client_id}'
    channel.queue_declare(queue=backward_queue_name, durable=False)

    message = pickle.dumps({"data_id": data_id, "data": gradient.detach().cpu().numpy(), "trace": trace})

    channel.basic_publish(
        exchange='',
        routing_key=backward_queue_name,
        body=message
    )
    # print("Sent gradient")


def stop_connection():
    connection.close()


def train_on_device():
    channel = connection.channel()
    forward_queue_name = f'intermediate_queue_{layer_id - 1}'
    channel.queue_declare(queue=forward_queue_name, durable=False)
    print('Waiting for intermediate output. To exit press CTRL+C')
    model.to(device)
    while True:
        # Training model
        model.train()
        optimizer.zero_grad()
        # Process gradient
        method_frame, header_frame, body = channel.basic_get(queue=forward_queue_name, auto_ack=True)
        if method_frame and body:
            # print("Received intermediate output")
            received_data = pickle.loads(body)
            intermediate_output_numpy = received_data["data"]
            trace = received_data["trace"]
            data_id = received_data["data_id"]

            labels = received_data["label"].to(device)
            intermediate_output = torch.tensor(intermediate_output_numpy, requires_grad=True).to(device)

            output = model(intermediate_output)
            loss = criterion(output, labels)
            print(f"Loss: {loss.item()}")
            intermediate_output.retain_grad()
            loss.backward()
            optimizer.step()

            gradient = intermediate_output.grad
            send_gradient(data_id, gradient, trace)  # 1F1B
        # Check training process
        else:
            broadcast_queue_name = 'broadcast_queue'
            method_frame, header_frame, body = channel.basic_get(queue=broadcast_queue_name, auto_ack=True)
            if body:
                received_data = pickle.loads(body)
                print(f"Received message from server {received_data}")
                break


if __name__ == "__main__":
    print("Client sending registration message to server...")
    data = {"action": "REGISTER", "client_id": client_id, "layer_id": layer_id, "message": "Hello from Client!"}
    client = RpcClient(client_id, layer_id, model, address, username, password, train_on_device)
    client.send_to_server(data)
