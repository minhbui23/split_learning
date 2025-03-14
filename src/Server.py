import os
import time
import pika
import pickle
import sys
import yaml
import numpy as np
import torch
import torch.nn as nn
import requests

from requests.auth import HTTPBasicAuth

import src.Model
import src.Log
import src.Utils
import src.Validation

num_labels = 10


def delete_old_queues(address, username, password):
    url = f'http://{address}:15672/api/queues'
    response = requests.get(url, auth=HTTPBasicAuth(username, password))

    if response.status_code == 200:
        queues = response.json()

        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(address, 5672, '/', credentials))
        http_channel = connection.channel()

        for queue in queues:
            queue_name = queue['name']
            if queue_name.startswith("reply") or queue_name.startswith("intermediate_queue") or queue_name.startswith(
                    "gradient_queue") or queue_name.startswith("rpc_queue"):
                try:
                    http_channel.queue_delete(queue=queue_name)
                    src.Log.print_with_color(f"Queue '{queue_name}' deleted.", "green")
                except Exception as e:
                    src.Log.print_with_color(f"Failed to delete queue '{queue_name}': {e}", "yellow")
            else:
                try:
                    http_channel.queue_purge(queue=queue_name)
                    src.Log.print_with_color(f"Queue '{queue_name}' purged.", "green")
                except Exception as e:
                    src.Log.print_with_color(f"Failed to purge queue '{queue_name}': {e}", "yellow")

        connection.close()
        return True
    else:
        src.Log.print_with_color(
            f"Failed to fetch queues from RabbitMQ Management API. Status code: {response.status_code}", "yellow")
        return False


class Server:
    def __init__(self, config_dir):
        with open(config_dir, 'r') as file:
            config = yaml.safe_load(file)

        address = config["rabbit"]["address"]
        username = config["rabbit"]["username"]
        password = config["rabbit"]["password"]
        delete_old_queues(address, username, password)

        self.model_name = config["server"]["model"]
        self.total_clients = config["server"]["clients"]
        self.cut_layers = config["server"]["cut_layers"]
        self.num_round = config["server"]["num-round"]
        self.round = self.num_round
        self.save_parameters = config["server"]["parameters"]["save"]
        self.load_parameters = config["server"]["parameters"]["load"]
        self.validation = config["server"]["validation"]

        # Clients
        self.batch_size = config["learning"]["batch-size"]
        self.lr = config["learning"]["learning-rate"]
        self.momentum = config["learning"]["momentum"]
        self.control_count = config["learning"]["control-count"]

        log_path = config["log_path"]
        num_data_per_label = config["learning"]["num_data_per_label"]
        self.label_count = [num_data_per_label // self.total_clients[0] for _ in range(num_labels)]
        self.time_start = None
        self.time_stop = None

        credentials = pika.PlainCredentials(username, password)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(address, 5672, '/', credentials))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue='rpc_queue')

        self.current_clients = [0 for _ in range(len(self.total_clients))]
        self.register_clients = [0 for _ in range(len(self.total_clients))]
        self.first_layer_clients = 0
        self.responses = {}  # Save response
        self.list_clients = []
        self.avg_state_dict = [[] for _ in range(len(self.total_clients))]
        self.round_result = True

        self.all_model_parameters = [[] for _ in range(len(self.total_clients))]
        self.all_client_sizes = [[] for _ in range(len(self.total_clients))]
        self.all_labels = np.array([])
        self.all_vals = np.array([])

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue='rpc_queue', on_message_callback=self.on_request)
        self.logger = src.Log.Logger(f"{log_path}/app.log")
        self.logger.log_info("Application start")

        src.Log.print_with_color(f"Server is waiting for {self.total_clients} clients.", "green")

    def on_request(self, ch, method, props, body):
        message = pickle.loads(body)
        routing_key = props.reply_to
        action = message["action"]
        client_id = message["client_id"]
        layer_id = message["layer_id"]
        self.responses[routing_key] = message
        if (str(client_id), layer_id) not in self.list_clients:
            self.list_clients.append((str(client_id), layer_id))

        if action == "REGISTER":
            src.Log.print_with_color(f"[<<<] Received message from client: {message}", "blue")
            # Save messages from clients
            self.register_clients[layer_id - 1] += 1

            # If consumed all clients - Register for first time
            if self.register_clients == self.total_clients:
                src.Log.print_with_color("All clients are connected. Sending notifications.", "green")
                src.Log.print_with_color(f"Start training round {self.num_round - self.round + 1}", "yellow")
                self.notify_clients()
        elif action == "NOTIFY":
            src.Log.print_with_color(f"[<<<] Received message from client: {message}", "blue")
            if layer_id == 1:
                self.first_layer_clients += 1
                validate = message["validate"]
                if validate:
                    self.all_labels = np.append(self.all_labels, validate[0])
                    self.all_vals = np.append(self.all_vals, validate[1])

            if self.first_layer_clients == self.total_clients[0]:
                self.first_layer_clients = 0
                self.time_stop = time.time_ns()

                t = self.time_stop - self.time_start
                self.logger.log_info(f"Training Time: {t} ns.")
                src.Log.print_with_color("Received finish training notification", "yellow")

                if self.all_labels.size == self.all_vals.size and self.all_vals.size > 0:
                    same_elements = np.sum(self.all_labels == self.all_vals)
                    total_elements = self.all_vals.size
                    accuracy = same_elements / total_elements
                    src.Log.print_with_color("Inference test: Accuracy: ({:.0f}%)".format(100.0 * accuracy), "yellow")
                    self.logger.log_info("Inference test: Accuracy: ({:.0f}%)\n".format(100.0 * accuracy))

                    self.all_labels = np.array([])
                    self.all_vals = np.array([])

                for (client_id, layer_id) in self.list_clients:
                    message = {"action": "PAUSE",
                               "message": "Pause training and please send your parameters",
                               "parameters": None}
                    self.send_to_response(client_id, pickle.dumps(message))
        elif action == "UPDATE":
            data_message = message["message"]
            result = message["result"]
            src.Log.print_with_color(f"[<<<] Received message from client: {data_message}", "blue")
            self.current_clients[layer_id - 1] += 1
            if not result:
                self.round_result = False

            # Save client's model parameters
            if self.save_parameters and self.round_result:
                model_state_dict = message["parameters"]
                client_size = message["size"]
                self.all_model_parameters[layer_id - 1].append(model_state_dict)
                self.all_client_sizes[layer_id - 1].append(client_size)

            # If consumed all client's parameters
            if self.current_clients == self.total_clients:
                src.Log.print_with_color("Collected all parameters.", "yellow")
                if self.save_parameters and self.round_result:
                    self.avg_all_parameters()
                    self.all_model_parameters = [[] for _ in range(len(self.total_clients))]
                    self.all_client_sizes = [[] for _ in range(len(self.total_clients))]
                self.current_clients = [0 for _ in range(len(self.total_clients))]
                # Test
                if self.save_parameters and self.validation and self.round_result:
                    state_dict_full = self.concatenate_state_dict()
                    if not src.Validation.test(self.model_name, state_dict_full, self.logger):
                        src.Log.print_with_color("Training failed!", "yellow")
                    else:
                        # Save to files
                        torch.save(state_dict_full, f'{self.model_name}.pth')
                        self.round -= 1
                else:
                    self.round -= 1

                # Start a new training round
                self.round_result = True

                if self.round > 0:
                    src.Log.print_with_color(f"Start training round {self.num_round - self.round + 1}", "yellow")
                    if self.save_parameters:
                        self.notify_clients()
                    else:
                        self.notify_clients(register=False)
                else:
                    self.notify_clients(start=False)
                    sys.exit()

        # Ack the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def notify_clients(self, start=True, register=True):
        # Send message to clients when consumed all clients
        klass = getattr(src.Model, self.model_name)
        full_model = klass()
        full_model = nn.Sequential(*nn.ModuleList(full_model.children()))
        for (client_id, layer_id) in self.list_clients:
            # Read parameters file
            filepath = f'{self.model_name}.pth'
            state_dict = None

            if start:
                if layer_id == 1:
                    layers = [0, self.cut_layers[0]]
                elif layer_id == len(self.total_clients):
                    layers = [self.cut_layers[-1], -1]
                else:
                    layers = [self.cut_layers[layer_id - 2], self.cut_layers[layer_id - 1]]

                if self.load_parameters and register:
                    if os.path.exists(filepath):
                        full_state_dict = torch.load(filepath, weights_only=True)
                        full_model.load_state_dict(full_state_dict)

                        if layer_id == 1:
                            model_part = nn.Sequential(*nn.ModuleList(full_model.children())[:layers[1]])
                        elif layer_id == len(self.total_clients):
                            model_part = nn.Sequential(*nn.ModuleList(full_model.children())[layers[0]:])
                        else:
                            model_part = nn.Sequential(*nn.ModuleList(full_model.children())[layers[0]:layers[1]])

                        state_dict = model_part.state_dict()
                        src.Log.print_with_color("Model loaded successfully.", "green")
                    else:
                        src.Log.print_with_color(f"File {filepath} does not exist.", "yellow")

                src.Log.print_with_color(f"[>>>] Sent start training request to client {client_id}", "red")
                response = {"action": "START",
                            "message": "Server accept the connection!",
                            "parameters": state_dict,
                            "num_layers": len(self.total_clients),
                            "layers": layers,
                            "model_name": self.model_name,
                            "control_count": self.control_count,
                            "batch_size": self.batch_size,
                            "lr": self.lr,
                            "momentum": self.momentum,
                            "label_count": self.label_count}
            else:
                src.Log.print_with_color(f"[>>>] Sent stop training request to client {client_id}", "red")
                response = {"action": "STOP",
                            "message": "Stop training!",
                            "parameters": None}
            self.time_start = time.time_ns()
            self.send_to_response(client_id, pickle.dumps(response))

    def start(self):
        self.channel.start_consuming()

    def send_to_response(self, client_id, message):
        reply_channel = self.connection.channel()
        reply_queue_name = f'reply_{client_id}'
        reply_channel.queue_declare(reply_queue_name, durable=False)

        src.Log.print_with_color(f"[>>>] Sent notification to client {client_id}", "red")
        reply_channel.basic_publish(
            exchange='',
            routing_key=reply_queue_name,
            body=message
        )

    def avg_all_parameters(self):
        # Average all client parameters
        for layer, state_dicts in enumerate(self.all_model_parameters):
            all_layer_client_size = self.all_client_sizes[layer]
            num_models = len(state_dicts)
            if num_models == 0:
                return

            self.avg_state_dict[layer] = state_dicts[0]

            for key in state_dicts[0].keys():
                if state_dicts[0][key].dtype != torch.long:
                    self.avg_state_dict[layer][key] = sum(state_dicts[i][key] * all_layer_client_size[i]
                                                          for i in range(num_models)) / sum(all_layer_client_size)
                else:
                    self.avg_state_dict[layer][key] = sum(state_dicts[i][key] * all_layer_client_size[i]
                                                          for i in range(num_models)) // sum(all_layer_client_size)

    def concatenate_state_dict(self):
        state_dict_full = {}
        for i, state_dicts in enumerate(self.avg_state_dict):
            if i > 0:
                state_dicts = src.Utils.change_state_dict(state_dicts, self.cut_layers[i - 1])
            state_dict_full.update(state_dicts)
        return state_dict_full
