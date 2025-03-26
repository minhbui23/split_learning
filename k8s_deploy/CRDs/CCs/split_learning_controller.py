import os
from kubernetes import client, config, watch
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Namespace để watch
NAMESPACE = os.getenv("WATCH_NAMESPACE", "split-learning")

# Kết nối với Kubernetes API
config.load_incluster_config()  # Dùng trong cluster
v1 = client.CoreV1Api()
custom_api = client.CustomObjectsApi()

def calculate_client_count(cpu, ram):

    ram_kib = int(ram.rstrip('Ki'))   
    ram_gib = ram_kib / (1024 * 1024)   

    cpu_based = int(cpu) / 1     
    ram_based = int(ram_gib / 2)          

    return min(cpu_based, ram_based)

def create_pod(node_name, client_count):
    """Tạo pod client trên node dựa trên cấu hình từ client_layer1.yaml."""
    for i in range(client_count):
        pod_name = f"split-client-{node_name}-{i}"
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": NAMESPACE,
                "labels": {"app": "client-layer1", "node": node_name}
            },
            "spec": {
                "tolerations": [{
                    "key": "node-role.kubernetes.io/control-plane",
                    "operator": "Exists",
                    "effect": "NoSchedule"
                }],
                "nodeSelector": {"kubernetes.io/hostname": node_name},
                "containers": [{
                    "name": "client",
                    "image": "minhbui1/client:v2.0.0",
                    "command": ["sh", "-c", "sleep 10 && python -u client.py --layer_id 1 --device cpu"],
                    "resources": {"limits": {"cpu": "1", "memory": "2Gi"}},
                    "volumeMounts": [{
                        "name": "config-volume",
                        "mountPath": "/app/config.yaml",
                        "subPath": "config.yaml"
                    }]
                }],
                "volumes": [{
                    "name": "config-volume",
                    "configMap": {"name": "split-learning-config"}
                }]
            }
        }
        try:
            v1.create_namespaced_pod(namespace=NAMESPACE, body=pod)
            logger.info(f"Created pod {pod_name}")
        except client.ApiException as e:
            logger.error(f"Error creating pod {pod_name}: {e}")

def delete_pods(node_name):
    """Xóa tất cả pod client trên node."""
    try:
        pods = v1.list_namespaced_pod(NAMESPACE, label_selector=f"app=client-layer1,node={node_name}")
        for pod in pods.items:
            v1.delete_namespaced_pod(pod.metadata.name, NAMESPACE)
            logger.info(f"Deleted pod {pod.metadata.name}")
    except client.ApiException as e:
        logger.error(f"Error deleting pods for node {node_name}: {e}")

def main():
    logger.info("Starting SplitLearningController")
    
    w = watch.Watch()
    for event in w.stream(v1.list_node):  # Không giới hạn timeout khi chạy thật
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
            
            try:
                # Tạo hoặc cập nhật ClientLayer1
                custom_api.get_namespaced_custom_object(
                    group="splitlearning.io", version="v1", namespace=NAMESPACE,
                    plural="clientlayer1s", name=cl1_name
                )
                custom_api.patch_namespaced_custom_object(
                    group="splitlearning.io", version="v1", namespace=NAMESPACE,
                    plural="clientlayer1s", name=cl1_name, body=cl1
                )
                logger.info(f"Updated ClientLayer1 {cl1_name}")
            except client.ApiException as e:
                if e.status == 404:
                    custom_api.create_namespaced_custom_object(
                        group="splitlearning.io", version="v1", namespace=NAMESPACE,
                        plural="clientlayer1s", body=cl1
                    )
                    logger.info(f"Created ClientLayer1 {cl1_name}")
                else:
                    logger.error(f"Error with ClientLayer1 {cl1_name}: {e}")

            # Deploy pod client
            delete_pods(node_name)  # Xóa pod cũ trước
            create_pod(node_name, client_count)

            # Cập nhật status
            status = {"status": {"activeClients": client_count}}
            try:
                custom_api.patch_namespaced_custom_object_status(
                    group="splitlearning.io", version="v1", namespace=NAMESPACE,
                    plural="clientlayer1s", name=cl1_name, body=status
                )
                logger.info(f"Updated status for ClientLayer1 {cl1_name}")
            except client.ApiException as e:
                logger.error(f"Error updating status for {cl1_name}: {e}")

        elif event_type == "DELETED":
            cl1_name = f"clientlayer1-{node_name}"
            delete_pods(node_name)
            try:
                custom_api.delete_namespaced_custom_object(
                    group="splitlearning.io", version="v1", namespace=NAMESPACE,
                    plural="clientlayer1s", name=cl1_name
                )
                logger.info(f"Deleted ClientLayer1 {cl1_name}")
            except client.ApiException as e:
                logger.error(f"Error deleting ClientLayer1 {cl1_name}: {e}")

if __name__ == "__main__":
    main()