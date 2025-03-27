# app_config.py
import os
import yaml
import logging
from kubernetes import client, config as k8s_config
from kubernetes.client import V1Taint  
from kubernetes.client.rest import ApiException

# --- Các biến toàn cục sẽ được export ---
# Khởi tạo với giá trị mặc định hoặc None trước khi load
# Clients & Logger
logger = None
v1 = None
custom_api = None
apps_v1_api = None

# Kubernetes Configs
NAMESPACE = "default"
KUBECONFIG_PATH = None # Giữ lại nếu cần tham chiếu sau này

# Controller Configs
LAYER_LABEL_KEY = None
LAYER_LABEL_VALUE = None
POD_APP_LABEL = "split-client"
CONTROLLER_LABEL_KEY = "controller"
CONTROLLER_LABEL_VALUE = "split-learning-controller"
NODE_LABEL_KEY = "node"
DEPLOYMENT_APP_LABEL = "split-client-deployment"

# CRD Configs
CRD_GROUP = None
CRD_VERSION = None
CRD_PLURAL = None

# Pod Template Configs
POD_TEMPLATE_CONFIGMAP_NAME = None
POD_TEMPLATE_CONFIGMAP_KEY = None

# Resource Request Configs 
CPU_REQ = "1"
RAM_REQ = "2" 

# Processed Node Taint Object
LAYER1_NODE_TAINT = None

# --- Hàm thực hiện load và khởi tạo ---
def load_config_and_initialize():
    """Tải config, khởi tạo clients, trích xuất và thiết lập các biến toàn cục."""
    # Sử dụng global để cập nhật các biến ở cấp module
    global logger, v1, custom_api, apps_v1_api, \
           NAMESPACE, KUBECONFIG_PATH, LAYER_LABEL_KEY, LAYER_LABEL_VALUE, \
           CRD_GROUP, CRD_VERSION, CRD_PLURAL, POD_TEMPLATE_CONFIGMAP_NAME, \
           POD_TEMPLATE_CONFIGMAP_KEY, POD_APP_LABEL, CONTROLLER_LABEL_KEY, \
           CONTROLLER_LABEL_VALUE, NODE_LABEL_KEY, DEPLOYMENT_APP_LABEL, \
           CPU_REQ, RAM_REQ, LAYER1_NODE_TAINT

    # 1. Setup Logging cơ bản (ưu tiên ENV VAR)
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("SplitLearningController")

    # 2. Load configuration file
    config_path = os.getenv("CONTROLLER_CONFIG_PATH", "config_controller.yaml")
    logger.info(f"Loading configuration from: {config_path}")
    if not os.path.exists(config_path):
        logger.critical(f"Configuration file not found at {config_path}. Exiting.")
        exit(1)

    config_dict = {} # Dùng biến cục bộ để load
    try:
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
            if not config_dict:
                logger.critical(f"Config file {config_path} is empty/invalid. Exiting.")
                exit(1)
            logger.info("Configuration YAML loaded successfully.")
    except (yaml.YAMLError, IOError) as e:
        logger.critical(f"Error loading/parsing config file {config_path}: {e}. Exiting.")
        exit(1)

    # 3. Cập nhật Log Level từ file config (nếu có)
    log_level_str_from_file = config_dict.get("logging", {}).get("level", log_level_str).upper()
    log_level_from_file = getattr(logging, log_level_str_from_file, log_level)
    if log_level_from_file != logger.level:
        logger.setLevel(log_level_from_file)
        logger.info(f"Log level updated to {log_level_str_from_file} from config file.")

    # 4. Khởi tạo Kubernetes API Clients
    try:
        KUBECONFIG_PATH = config_dict.get("kubernetes", {}).get("kubeconfig_path") # Gán vào biến toàn cục nếu cần
        if KUBECONFIG_PATH:
            logger.info(f"Loading kube config from: {KUBECONFIG_PATH}")
            k8s_config.load_kube_config(config_file=KUBECONFIG_PATH)
        else:
            # Giữ nguyên logic load in-cluster/default của bạn
            logger.info("kubeconfig_path not specified, attempting default load...")
            try:
                k8s_config.load_incluster_config()
                logger.info("Loaded in-cluster config.")
            except k8s_config.ConfigException:
                logger.info("In-cluster config failed, trying default kube config.")
                k8s_config.load_kube_config()
                logger.info("Loaded default kube config.")

        v1 = client.CoreV1Api()
        custom_api = client.CustomObjectsApi()
        apps_v1_api = client.AppsV1Api()
        logger.info("Kubernetes API clients initialized.")
    except k8s_config.ConfigException as e:
         logger.critical(f"Could not configure Kubernetes client: {e}. Exiting.")
         exit(1)
    except Exception as e:
         logger.critical(f"Unexpected error initializing Kubernetes client: {e}. Exiting.")
         exit(1)

    # 5. Trích xuất các giá trị cấu hình vào biến toàn cục
    # --- Kubernetes ---
    NAMESPACE = config_dict.get("kubernetes", {}).get("namespace", "default")

    # --- Controller ---
    controller_cfg = config_dict.get("controller", {})
    LAYER_LABEL_KEY = controller_cfg.get("layer_label_key")
    LAYER_LABEL_VALUE = controller_cfg.get("layer_label_value")
    POD_APP_LABEL = controller_cfg.get("pod_app_label", "split-client")
    CONTROLLER_LABEL_KEY = controller_cfg.get("controller_label_key", "controller")
    CONTROLLER_LABEL_VALUE = controller_cfg.get("controller_label_value", "split-learning-controller")
    NODE_LABEL_KEY = controller_cfg.get("node_label_key", "node")
    DEPLOYMENT_APP_LABEL = controller_cfg.get("deployment_app_label", "split-client-deployment")

    # --- CRD ---
    crd_cfg = config_dict.get("crd", {})
    CRD_GROUP = crd_cfg.get("group")
    CRD_VERSION = crd_cfg.get("version")
    CRD_PLURAL = crd_cfg.get("plural")

    # --- Pod Template ---
    pod_template_cfg = config_dict.get("pod_template", {})
    POD_TEMPLATE_CONFIGMAP_NAME = pod_template_cfg.get("configmap_name")
    POD_TEMPLATE_CONFIGMAP_KEY = pod_template_cfg.get("configmap_key")

    # --- Resource Requests ---
    resource_cfg = config_dict.get("resource_requests", {})
    CPU_REQ = resource_cfg.get("client_cpu", "1")
    RAM_REQ = resource_cfg.get("client_memory", "2") # Giả định 2Gi là default hợp lý

    # --- Node Taint (Trích xuất và Tạo đối tượng V1Taint) ---
    node_taint_cfg = controller_cfg.get("node_taint", {})
    node_taint_key = node_taint_cfg.get("key")
    node_taint_value = node_taint_cfg.get("value") # Có thể None
    node_taint_effect = node_taint_cfg.get("effect")

    if node_taint_key and node_taint_effect:
        # Tạo đối tượng V1Taint và gán vào biến toàn cục
        LAYER1_NODE_TAINT = V1Taint(
            key=node_taint_key,
            value=node_taint_value,
            effect=node_taint_effect
        )
        logger.info(f"Layer 1 Node Taint config loaded: Key={node_taint_key}, Value={node_taint_value}, Effect={node_taint_effect}")
    else:
        logger.warning("Layer 1 Node Taint 'key' or 'effect' missing in configuration. Taint operations will be skipped.")
        LAYER1_NODE_TAINT = None # Đảm bảo là None nếu thiếu

    # 6. Kiểm tra các cấu hình bắt buộc
    critical_configs = {
        "LAYER_LABEL_KEY": LAYER_LABEL_KEY,
        "LAYER_LABEL_VALUE": LAYER_LABEL_VALUE,
        "CRD_GROUP": CRD_GROUP,
        "CRD_VERSION": CRD_VERSION,
        "CRD_PLURAL": CRD_PLURAL,
        # Thêm các key bắt buộc khác nếu có
    }
    missing_critical = [k for k, v in critical_configs.items() if not v]
    if missing_critical:
        logger.critical(f"Missing critical configuration keys in {config_path}: {', '.join(missing_critical)}. Exiting.")
        exit(1)

    logger.info("All configurations processed and global variables populated.")

# --- Chạy hàm khởi tạo ngay khi module được import ---
load_config_and_initialize()

# --- Định nghĩa những gì được export công khai ---
# Chỉ export các biến/đối tượng đã xử lý, không export config_dict thô
__all__ = [
    # Logger & Clients
    "logger", "v1", "custom_api", "apps_v1_api",
    # Processed Config Values
    "NAMESPACE", "KUBECONFIG_PATH", # Export KUBECONFIG_PATH nếu cần dùng ở đâu đó
    "LAYER_LABEL_KEY", "LAYER_LABEL_VALUE", "POD_APP_LABEL", "CONTROLLER_LABEL_KEY",
    "CONTROLLER_LABEL_VALUE", "NODE_LABEL_KEY", "DEPLOYMENT_APP_LABEL",
    "CRD_GROUP", "CRD_VERSION", "CRD_PLURAL",
    "POD_TEMPLATE_CONFIGMAP_NAME", "POD_TEMPLATE_CONFIGMAP_KEY",
    "CPU_REQ", "RAM_REQ",
    "LAYER1_NODE_TAINT" # Export đối tượng Taint đã tạo
]

print(f"DEBUG: app_config loaded. Namespace: {NAMESPACE}, Taint: {LAYER1_NODE_TAINT}") # Thêm log debug nếu cần