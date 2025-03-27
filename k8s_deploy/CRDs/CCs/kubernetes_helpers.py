# kubernetes_helpers.py
import yaml
from kubernetes import client
from kubernetes.client.rest import ApiException

# Import các biến và client từ config
from app_config import (
    logger, v1, custom_api, apps_v1_api, # Clients & logger
    NAMESPACE, CRD_GROUP, CRD_VERSION, CRD_PLURAL, # CRD info
    POD_TEMPLATE_CONFIGMAP_NAME, POD_TEMPLATE_CONFIGMAP_KEY, # Pod template info
    POD_APP_LABEL, CONTROLLER_LABEL_KEY, CONTROLLER_LABEL_VALUE, # Labels
    NODE_LABEL_KEY, DEPLOYMENT_APP_LABEL,
    LAYER1_NODE_TAINT # Import Taint object nếu helper cần dùng trực tiếp
)

NODE_TAINT_KEY = LAYER1_NODE_TAINT.key
NODE_TAINT_VALUE = LAYER1_NODE_TAINT.value
NODE_TAINT_EFFECT = LAYER1_NODE_TAINT.effect

def ensure_node_taint(node_name, node_spec, desired_taint):
    """Kiểm tra và áp dụng Taint nếu cần thiết."""
    current_taints = node_spec.taints if node_spec.taints else []
    taint_exists = any(
        t.key == desired_taint.key and t.effect == desired_taint.effect
        for t in current_taints
    )
    if not taint_exists:
        logger.info(
            f"Taint {desired_taint.key}={desired_taint.value}:{desired_taint.effect} not found on node {node_name}. Applying..."
        )
        new_taints = current_taints + [desired_taint]
        patch_body = {"spec": {"taints": [t.to_dict() for t in new_taints]}}
        try:
            v1.patch_node(node_name, patch_body)
            logger.info(f"Successfully applied taint to node {node_name}")
            return True
        except ApiException as e:
            logger.error(
                f"Error applying taint to node {node_name}: {e.status} - {e.reason} - {e.body}"
            )
            return False
    else:
        return True


# --- Hàm liên quan đến Deployment ---

def _get_pod_template_from_configmap():
    """Đọc và parse template Pod từ ConfigMap."""
    if not POD_TEMPLATE_CONFIGMAP_NAME or not POD_TEMPLATE_CONFIGMAP_KEY:
        logger.error("Pod template ConfigMap name or key not configured.")
        return None
    try:
        cm = v1.read_namespaced_config_map(
            name=POD_TEMPLATE_CONFIGMAP_NAME, namespace=NAMESPACE
        )
        template_yaml_str = cm.data.get(POD_TEMPLATE_CONFIGMAP_KEY)
        if not template_yaml_str:
            logger.error(
                f"Key '{POD_TEMPLATE_CONFIGMAP_KEY}' not found in ConfigMap '{POD_TEMPLATE_CONFIGMAP_NAME}'."
            )
            return None
        pod_template_spec = yaml.safe_load(template_yaml_str)
        # Cần validate cấu trúc YAML của template ở đây nếu cần
        # Chỉ lấy phần 'template' của Pod (thường là spec không có metadata ngoài)
        if isinstance(pod_template_spec, dict) and "spec" in pod_template_spec:
            # Giả sử CM chứa spec của Pod
            return pod_template_spec["spec"]
        elif isinstance(pod_template_spec, dict):
            # Giả sử CM chỉ chứa phần spec
            return pod_template_spec
        else:
            logger.error("Pod template YAML from ConfigMap has invalid format.")
            return None

    except ApiException as e:
        if e.status == 404:
            logger.error(
                f"ConfigMap '{POD_TEMPLATE_CONFIGMAP_NAME}' not found in namespace '{NAMESPACE}'."
            )
        else:
            logger.error(
                f"Error reading ConfigMap '{POD_TEMPLATE_CONFIGMAP_NAME}': {e.status} - {e.reason}"
            )
        return None
    except yaml.YAMLError as e:
        logger.error(
            f"Error parsing Pod template YAML from ConfigMap '{POD_TEMPLATE_CONFIGMAP_NAME}': {e}"
        )
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting pod template from ConfigMap: {e}")
        return None


def get_deployment(deployment_name):
    """Lấy Deployment theo tên."""
    try:
        return apps_v1_api.read_namespaced_deployment(
            name=deployment_name, namespace=NAMESPACE
        )
    except ApiException as e:
        if e.status == 404:
            return None
        logger.error(
            f"Error getting Deployment {deployment_name}: {e.status} - {e.reason}"
        )
        raise


def create_deployment(deployment_name, node_name, replicas, owner_ref):
    """Tạo mới Deployment."""
    pod_template_spec = _get_pod_template_from_configmap()
    if not pod_template_spec:
        logger.error(
            f"Cannot create Deployment {deployment_name}, failed to get Pod template."
        )
        return None

    # --- Định nghĩa Labels và Selectors ---
    # Labels cho Deployment và Pods để Deployment tìm thấy Pods của nó
    pod_labels = {
        POD_APP_LABEL: "true",
        NODE_LABEL_KEY: node_name,
        # Thêm label để Deployment biết Pod này là của nó
        DEPLOYMENT_APP_LABEL: deployment_name,  # hoặc một label cố định nếu muốn
    }
    # Selector để Deployment khớp với Pods
    match_labels = {DEPLOYMENT_APP_LABEL: deployment_name}  # hoặc label cố định đã dùng

    # --- Thêm Tolerations vào Pod template spec ---
    if all([NODE_TAINT_KEY, NODE_TAINT_EFFECT]):
        toleration = client.V1Toleration(
            key=NODE_TAINT_KEY,
            operator="Equal",  # hoặc Exists nếu value không quan trọng
            value=NODE_TAINT_VALUE,
            effect=NODE_TAINT_EFFECT,
        )
        if "tolerations" not in pod_template_spec:
            pod_template_spec["tolerations"] = []
        # Tránh thêm trùng lặp nếu template đã có sẵn
        if not any(
            t.key == toleration.key and t.effect == toleration.effect
            for t in pod_template_spec["tolerations"]
        ):
            pod_template_spec["tolerations"].append(
                toleration.to_dict()
            )  # Chuyển thành dict nếu cần

    # --- Thêm Node Selector hoặc Affinity vào Pod template spec ---
    # Ưu tiên NodeAffinity hơn nodeSelector
    node_selector_term = client.V1NodeSelectorTerm(
        match_expressions=[
            client.V1NodeSelectorRequirement(
                key="kubernetes.io/hostname", operator="In", values=[node_name]
            )
        ]
    )
    node_selector = client.V1NodeSelector(node_selector_terms=[node_selector_term])
    required_affinity = client.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=node_selector
    )
    affinity = client.V1Affinity(node_affinity=required_affinity)

    if "affinity" not in pod_template_spec:
        pod_template_spec["affinity"] = affinity.to_dict()
    else:
        # Cần logic merge phức tạp hơn nếu affinity đã tồn tại
        logger.warning(
            f"Pod template already has affinity rules. Overwriting/Merging logic might be needed."
        )
        pod_template_spec["affinity"] = affinity.to_dict()  # Ghi đè đơn giản

    # --- Tạo Deployment Manifest ---
    deployment_manifest = client.V1Deployment(
        api_version="apps/v1",
        kind="Deployment",
        metadata=client.V1ObjectMeta(
            name=deployment_name,
            namespace=NAMESPACE,
            owner_references=[owner_ref],  # OwnerReference là CRD
            labels={
                DEPLOYMENT_APP_LABEL: "true",
                NODE_LABEL_KEY: node_name,
            },  # Labels cho chính Deployment
        ),
        spec=client.V1DeploymentSpec(
            replicas=replicas,
            selector=client.V1LabelSelector(match_labels=match_labels),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=pod_labels),  # Labels cho Pods
                spec=pod_template_spec,  # Sử dụng spec từ ConfigMap đã chỉnh sửa
            ),
        ),
    )

    try:
        api_response = apps_v1_api.create_namespaced_deployment(
            body=deployment_manifest, namespace=NAMESPACE
        )
        logger.info(f"Successfully created Deployment {deployment_name}")
        return api_response
    except ApiException as e:
        logger.error(
            f"Error creating Deployment {deployment_name}: {e.status} - {e.reason} - {e.body}"
        )
        raise  # Ném lỗi để hàm gọi xử lý


def patch_deployment_replicas(deployment_name, replicas):
    """Patch số replicas của Deployment."""
    patch_body = {"spec": {"replicas": replicas}}
    try:
        api_response = apps_v1_api.patch_namespaced_deployment_scale(
            name=deployment_name, namespace=NAMESPACE, body=patch_body
        )
        logger.info(
            f"Successfully patched replicas for Deployment {deployment_name} to {replicas}"
        )
        return api_response
    except ApiException as e:
        logger.error(
            f"Error patching replicas for Deployment {deployment_name}: {e.status} - {e.reason} - {e.body}"
        )
        raise


def delete_deployment(deployment_name):
    """Xóa Deployment."""
    try:
        apps_v1_api.delete_namespaced_deployment(
            name=deployment_name,
            namespace=NAMESPACE,
            body=client.V1DeleteOptions(
                propagation_policy="Foreground"
            ),  # Đảm bảo owner xóa xong thì resource con mới bị xóa
        )
        logger.info(f"Successfully initiated deletion of Deployment {deployment_name}")
        return True
    except ApiException as e:
        if e.status == 404:
            logger.warning(f"Deployment {deployment_name} not found for deletion.")
            return True
        logger.error(
            f"Error deleting Deployment {deployment_name}: {e.status} - {e.reason}"
        )
        return False


# --- Hàm liên quan đến CRD ---


def get_crd(crd_name):
    """Lấy CRD ClientLayer1 theo tên."""
    try:
        return custom_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=crd_name,
        )
    except ApiException as e:
        if e.status == 404:
            return None  # Trả về None nếu không tìm thấy
        logger.error(f"Error getting CRD {crd_name}: {e.status} - {e.reason}")
        raise  # Ném lại lỗi để hàm gọi xử lý
    except Exception as e_gen:
        logger.error(f"Unknown error getting CRD {crd_name}: {e_gen}")
        raise


def create_crd(crd_body):
    """Tạo mới CRD ClientLayer1."""
    try:
        crd_name = crd_body.get("metadata", {}).get("name", "UNKNOWN")
        created_crd = custom_api.create_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            body=crd_body,
        )
        logger.info(f"Successfully created CRD {crd_name}")
        return created_crd
    except ApiException as e:
        crd_name = crd_body.get("metadata", {}).get("name", "UNKNOWN")
        logger.error(f"Error creating CRD {crd_name}: {e.status} - {e.reason}")
        raise
    except Exception as e_gen:
        crd_name = crd_body.get("metadata", {}).get("name", "UNKNOWN")
        logger.error(f"Unknown error creating CRD {crd_name}: {e_gen}")
        raise


def patch_crd_spec(crd_name, spec_body):
    """Patch trường spec của CRD ClientLayer1."""
    try:
        patched_crd = custom_api.patch_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=crd_name,
            body={"spec": spec_body},
        )
        logger.info(f"Patch CRD {crd_name} spec successful.")
        return patched_crd
    except ApiException as e:
        logger.error(f"Error patching CRD {crd_name} spec: {e.status} - {e.reason}")
        raise
    except Exception as e_gen:
        logger.error(f"Unknown error patching CRD spec {crd_name}: {e_gen}")
        raise


def update_crd_status(crd_name, active_count, desired_count):
    """Cập nhật trường status của CRD ClientLayer1."""
    status_body = {
        "status": {"activeClients": active_count, "desiredClients": desired_count}
    }
    try:
        custom_api.patch_namespaced_custom_object_status(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=crd_name,
            body=status_body,
        )
        logger.info(
            f"Updated status for CRD {crd_name} (active: {active_count}, desired: {desired_count})"
        )
        return True
    except ApiException as e:
        logger.error(
            f"Error updating status for CRD {crd_name}: {e.status} - {e.reason}"
        )
        if e.status == 409:
            logger.warning(f"Conflict updating status for CRD {crd_name}.")
        return False  # Không raise lỗi ở status update
    except Exception as e_gen:
        logger.error(f"Unknown error updating status CRD {crd_name}: {e_gen}")
        return False


def delete_crd(crd_name):
    """Xóa CRD ClientLayer1."""
    try:
        logger.info(f"Deleting ClientLayer1 CRD {crd_name}...")
        custom_api.delete_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=NAMESPACE,
            plural=CRD_PLURAL,
            name=crd_name,
            body=client.V1DeleteOptions(),
        )
        logger.info(f"Successfully initiated deletion of CRD {crd_name}")
        return True
    except ApiException as e:
        if e.status == 404:
            logger.warning(f"CRD {crd_name} not found for deletion.")
            return True  # Coi như thành công nếu không tìm thấy
        logger.error(f"Error deleting CRD {crd_name}: {e.status} - {e.reason}")
        return False
    except Exception as e_gen:
        logger.error(f"Unknown error deleting CRD {crd_name}: {e_gen}")
        return False
