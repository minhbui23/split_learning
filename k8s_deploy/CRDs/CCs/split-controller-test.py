import os
from kubernetes import client, config, watch
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Namespace để watch
NAMESPACE = os.getenv("WATCH_NAMESPACE", "split-learning")

# Kết nối với Kubernetes API từ kubeconfig local (cho test)
config.load_kube_config()  # Thay bằng load_incluster_config() khi deploy
v1 = client.CoreV1Api()
custom_api = client.CustomObjectsApi()

def calculate_client_count(cpu, ram):
    """Tính clientCount dựa trên tài nguyên CPU (mCPU) và RAM (Mi)."""
    cpu_milli = int(cpu.rstrip('m'))  # Chuyển mCPU thành số nguyên
    ram_mb = int(ram.rstrip('Mi')) / 1024  # Chuyển Mi thành GiB
    cpu_based = int(cpu_milli / 1000)  # 1000mCPU = 1CPU
    ram_based = int(ram_mb / 2)  # 2Gi RAM = 1 client
    return min(cpu_based, ram_based)

def create_pod(node_name, client_count):
    """Chỉ in log thay vì tạo pod để test."""
    for i in range(client_count):
        pod_name = f"split-client-{node_name}-{i}"
        logger.info(f"[TEST] Would create pod {pod_name} on node {node_name}")

def delete_pods(node_name):
    """Chỉ in log thay vì xóa pod để test."""
    logger.info(f"[TEST] Would delete pods on node {node_name}")

def main():
    logger.info("Starting SplitLearningController in test mode")
    
    # Kiểm tra kết nối tới cụm
    try:
        nodes = v1.list_node()
        logger.info(f"Connected to cluster. Found {len(nodes.items)} nodes.")
    except Exception as e:
        logger.error(f"Failed to connect to cluster: {e}")
        return

    w = watch.Watch()
    for event in w.stream(v1.list_node):  # Giới hạn 60s để test
        event_type = event["type"]
        node = event["object"]
        node_name = node.metadata.name
        cpu = node.status.allocatable["cpu"]
        ram = node.status.allocatable["memory"]
        labels = node.metadata.labels or {}

        # Kiểm tra label splitlearning.io/layerid
        layer_id = labels.get("splitlearning.io/layerid")

        logger.info(f"Event: {event_type} for node {node_name} (CPU: {cpu}, RAM: {ram}, LayerID: {layer_id})")

        # Nếu không có label layerid, bỏ qua
        if layer_id is None:
            logger.info(f"Skipping node {node_name} (no layerid label found)")
            continue

        # Chỉ xử lý layerid=1, bỏ qua layerid=2, 3
        if layer_id != "1":
            logger.info(f"Skipping node {node_name} (layerid={layer_id} not supported yet)")
            continue

        # Xử lý logic cho Layer 1
        if event_type in ["ADDED", "MODIFIED"]:
            client_count = calculate_client_count(cpu, ram)
            cl1_name = f"clientlayer1-{node_name}"
            cl1 = {
                "apiVersion": "splitlearning.io/v1",
                "kind": "ClientLayer1",
                "metadata": {"name": cl1_name, "namespace": NAMESPACE},
                "spec": {"nodeName": node_name, "clientCount": client_count}
            }
            
            logger.info(f"[TEST] Would create/update ClientLayer1 {cl1_name} with {client_count} clients")
            delete_pods(node_name)
            create_pod(node_name, client_count)
            logger.info(f"[TEST] Would update status activeClients to {client_count}")

        elif event_type == "DELETED":
            cl1_name = f"clientlayer1-{node_name}"
            delete_pods(node_name)
            logger.info(f"[TEST] Would delete ClientLayer1 {cl1_name}")

if __name__ == "__main__":
    main()