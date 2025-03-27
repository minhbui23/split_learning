from kubernetes import client
from kubernetes.client.rest import ApiException

from app_config import (
        logger, v1, custom_api, apps_v1_api, # Clients
        NAMESPACE, CRD_GROUP, CRD_VERSION, CRD_PLURAL, # CRD info
        LAYER1_NODE_TAINT, # Đối tượng Taint đã xử lý
        POD_TEMPLATE_CONFIGMAP_NAME, POD_TEMPLATE_CONFIGMAP_KEY, # Template info
        POD_APP_LABEL, NODE_LABEL_KEY, DEPLOYMENT_APP_LABEL # Labels
)

# Import config utils
from utils import calculate_client_count

from kubernetes_helpers import (
    ensure_node_taint,
    get_crd,
    create_crd,
    patch_crd_spec,
    update_crd_status,
    delete_crd,
    get_deployment,
    create_deployment,
    patch_deployment_replicas
)


def handle_added_layer1_node(node_name, node_spec, node_status):
    """Xử lý khi Node Layer 1 MỚI được thêm ."""
    logger.info(f"[Reconcile ADDED START] Node: {node_name}")

    # --- Bước 1: Đảm bảo Taint ---
    if not ensure_node_taint(node_name, node_spec, LAYER1_NODE_TAINT):
        logger.error(
            f"[Reconcile ADDED FAIL] Failed to ensure taint on node {node_name}."
        )
        return

    # --- Bước 2: Tính toán số lượng client mong muốn (replicas) ---
    try:
        cpu_alloc = node_status.allocatable.get("cpu")
        ram_alloc = node_status.allocatable.get("memory")
        if cpu_alloc is None or ram_alloc is None:
            raise KeyError("cpu or memory missing in node allocatable status")
        desired_replicas = calculate_client_count(cpu_alloc, ram_alloc)
    except (KeyError, TypeError, Exception) as e:
        logger.error(
            f"[Reconcile ADDED FAIL] Node {node_name}: Error calculating count: {e}"
        )
        return
    logger.info(f"Node {node_name}: Calculated desired replicas = {desired_replicas}")

    # --- Bước 3: Tạo CRD Mới ---
    crd_name = f"clientlayer1-{node_name}"
    crd_spec = {"nodeName": node_name, "desiredClients": desired_replicas}
    current_crd = None
    owner_ref_dict = None
    crd_uid = None

    try:
        # Kiểm tra xem CRD có vô tình tồn tại không (race condition hoặc trạng thái cũ)
        existing_crd = get_crd(crd_name)
        if existing_crd:
            logger.warning(
                f"CRD {crd_name} already exists during ADDED event. Attempting to adopt/reconcile."
            )
            # Có thể gọi handle_modified_layer1_node ở đây hoặc chỉ log và tiếp tục
            # Tùy chọn đơn giản: Sử dụng CRD hiện có
            current_crd = existing_crd
            crd_uid = current_crd.get("metadata", {}).get("uid")
            # Có thể cần patch spec nếu khác biệt
            current_spec = current_crd.get("spec", {})
            if current_spec != crd_spec:
                logger.info(f"Patching existing CRD {crd_name} spec during ADDED flow.")
                try:
                    current_crd = patch_crd_spec(crd_name, crd_spec)
                except Exception as patch_err:
                    logger.error(
                        f"[Reconcile ADDED FAIL] Failed to patch existing CRD {crd_name}: {patch_err}"
                    )
                    return
        else:
            # Tạo CRD mới như mong đợi trong luồng ADDED
            logger.info(f"Creating new CRD {crd_name}...")
            crd_body = {
                "apiVersion": f"{CRD_GROUP}/{CRD_VERSION}",
                "kind": "ClientLayer1",
                "metadata": {"name": crd_name, "namespace": NAMESPACE},
                "spec": crd_spec,
            }
            try:
                current_crd = create_crd(crd_body)
                logger.info(f"Successfully created CRD {crd_name}")
                crd_uid = current_crd.get("metadata", {}).get("uid")
            except Exception as create_err:
                # Nếu tạo lỗi (ví dụ conflict do tạo đồng thời), có thể thử get lại
                if isinstance(create_err, ApiException) and create_err.status == 409:
                    logger.warning(
                        f"Conflict creating CRD {crd_name}. Trying to get existing one."
                    )
                    current_crd = get_crd(crd_name)
                    if not current_crd:
                        logger.error(
                            f"[Reconcile ADDED FAIL] Failed create CRD {crd_name} due to conflict and couldn't get it afterwards."
                        )
                        return
                    crd_uid = current_crd.get("metadata", {}).get("uid")
                    # Patch spec nếu cần
                    current_spec = current_crd.get("spec", {})
                    if current_spec != crd_spec:
                        logger.info(
                            f"Patching CRD {crd_name} spec after creation conflict."
                        )
                        try:
                            current_crd = patch_crd_spec(crd_name, crd_spec)
                        except Exception as patch_err_post_conflict:
                            logger.error(
                                f"[Reconcile ADDED FAIL] Failed patch CRD {crd_name} after conflict: {patch_err_post_conflict}"
                            )
                            return
                else:
                    logger.error(
                        f"[Reconcile ADDED FAIL] Failed to create CRD {crd_name}: {create_err}."
                    )
                    return

        # --- Lấy Owner Reference ---
        if not crd_uid:
            logger.error(
                f"[Reconcile ADDED FAIL] Failed to get UID for CRD {crd_name}."
            )
            return
        owner_ref_dict = client.V1OwnerReference(
            api_version=current_crd["apiVersion"],
            kind=current_crd["kind"],
            name=current_crd["metadata"]["name"],
            uid=crd_uid,
            controller=True,
            block_owner_deletion=True,
        )

    except Exception as crd_err:
        logger.error(
            f"[Reconcile ADDED FAIL] Error processing CRD {crd_name}: {crd_err}."
        )
        return

    # --- Bước 4: Tạo Deployment Mới ---
    deployment_name = f"client-{node_name}-deployment"
    try:
        # Kiểm tra Deployment có tồn tại không (không nên xảy ra trong luồng ADDED thuần túy)
        existing_deployment = get_deployment(deployment_name)
        if existing_deployment:
            logger.warning(
                f"Deployment {deployment_name} already exists during ADDED event. Attempting to adopt/update."
            )
            # Đảm bảo OwnerRef đúng
            updated = False
            if not any(
                owner.uid == crd_uid
                for owner in existing_deployment.metadata.owner_references or []
            ):
                logger.warning(
                    f"Deployment {deployment_name} missing correct OwnerReference. Patching OwnerRef."
                )
                # Cần patch metadata.ownerReferences (logic phức tạp hơn)
                # Tạm thời bỏ qua patch OwnerRef phức tạp, chỉ patch replicas nếu cần
            # Cập nhật replicas nếu khác
            if existing_deployment.spec.replicas != desired_replicas:
                logger.info(
                    f"Patching existing Deployment {deployment_name} replicas during ADDED flow."
                )
                try:
                    patch_deployment_replicas(deployment_name, desired_replicas)
                    updated = True
                except Exception as patch_dep_err:
                    logger.error(
                        f"[Reconcile ADDED WARN] Failed patch existing Deployment {deployment_name} replicas: {patch_dep_err}"
                    )
            if not updated:
                logger.info(
                    f"Existing Deployment {deployment_name} seems correct during ADDED flow."
                )

        else:
            # Tạo Deployment mới
            logger.info(f"Creating new Deployment {deployment_name}...")
            try:
                create_deployment(
                    deployment_name, node_name, desired_replicas, owner_ref_dict
                )
            except Exception as create_dep_err:
                # Xử lý lỗi tạo Deployment (có thể do conflict)
                if (
                    isinstance(create_dep_err, ApiException)
                    and create_dep_err.status == 409
                ):
                    logger.warning(
                        f"Conflict creating Deployment {deployment_name}. Assuming it exists now."
                    )
                    # Có thể thử get lại và patch replicas nếu cần
                else:
                    logger.error(
                        f"[Reconcile ADDED FAIL] Failed to create Deployment {deployment_name}: {create_dep_err}."
                    )
                    # Cân nhắc xóa CRD vừa tạo nếu Deployment không tạo được? Hoặc để lần reconcile sau xử lý.
                    return

    except Exception as dep_err:
        logger.error(
            f"[Reconcile ADDED FAIL] Error processing Deployment {deployment_name}: {dep_err}."
        )
        return

    # --- Bước 5: Cập nhật Status của CRD ---
    # Ngay sau khi tạo, active_replicas có thể là 0
    active_replicas = 0
    # Cố gắng lấy trạng thái Deployment vừa tạo/cập nhật để có con số tốt hơn
    try:
        final_deployment = get_deployment(deployment_name)
        if final_deployment and final_deployment.status:
            active_replicas = final_deployment.status.ready_replicas or 0
    except Exception:
        logger.warning(
            f"Could not get immediate status for Deployment {deployment_name}. Reporting 0 active replicas initially."
        )

    if not update_crd_status(crd_name, active_replicas, desired_replicas):
        logger.warning(
            f"[Reconcile ADDED WARN] Failed to update status for CRD {crd_name}."
        )

    logger.info(f"[Reconcile ADDED END] Successfully processed node {node_name}.")


# --- Hàm xử lý cho sự kiện MODIFIED ---
def handle_modified_layer1_node(node_name, node_spec, node_status):
    """Xử lý khi Node Layer 1 bị thay đổi tài nguyên."""
    logger.info(f"[Reconcile MODIFIED START] Node: {node_name}")

    # --- Bước 1: Đảm bảo Taint ---
    # Vẫn cần đảm bảo Taint phòng trường hợp bị xóa thủ công
    if not ensure_node_taint(node_name, node_spec, LAYER1_NODE_TAINT):
        logger.error(
            f"[Reconcile MODIFIED FAIL] Failed to ensure taint on node {node_name}."
        )
        # Có thể không cần return ngay, tùy thuộc mức độ nghiêm trọng
        # return

    # --- Bước 2: Tính toán số lượng client mong muốn (replicas) ---
    try:
        cpu_alloc = node_status.allocatable.get("cpu")
        ram_alloc = node_status.allocatable.get("memory")
        if cpu_alloc is None or ram_alloc is None:
            raise KeyError("cpu or memory missing in node allocatable status")
        desired_replicas = calculate_client_count(cpu_alloc, ram_alloc)
    except (KeyError, TypeError, Exception) as e:
        logger.error(
            f"[Reconcile MODIFIED FAIL] Node {node_name}: Error calculating count: {e}"
        )
        return
    logger.info(f"Node {node_name}: Calculated desired replicas = {desired_replicas}")

    # --- Bước 3: Lấy CRD hiện có và Patch nếu cần ---
    crd_name = f"clientlayer1-{node_name}"
    crd_spec_update = {"nodeName": node_name, "desiredClients": desired_replicas}
    current_crd = None
    owner_ref_dict = None
    crd_uid = None

    try:
        current_crd = get_crd(crd_name)
        if not current_crd:
            # Lỗi: Node là Layer 1 nhưng CRD không tồn tại?
            logger.error(
                f"[Reconcile MODIFIED FAIL] CRD {crd_name} not found for existing Layer 1 node {node_name}. Inconsistent state."
            )
            # Có thể thử gọi handle_added_layer1_node để tạo lại?
            logger.info(
                f"Attempting to run ADDED logic for node {node_name} due to missing CRD."
            )
            handle_added_layer1_node(node_name, node_spec, node_status)
            return

        crd_uid = current_crd.get("metadata", {}).get("uid")
        current_spec = current_crd.get("spec", {})

        # Patch CRD spec nếu số client mong muốn thay đổi
        if current_spec.get("desiredClients") != desired_replicas:
            logger.info(
                f"CRD {crd_name} spec needs update (desiredClients: {current_spec.get('desiredClients')} -> {desired_replicas}). Patching..."
            )
            try:
                current_crd = patch_crd_spec(
                    crd_name, crd_spec_update
                )  # Cập nhật cả nodeName nếu cần
                logger.info(f"Patch CRD {crd_name} spec successful.")
            except Exception as patch_err:
                logger.error(
                    f"[Reconcile MODIFIED FAIL] Failed to patch CRD {crd_name} spec: {patch_err}."
                )
                return  # Lỗi patch CRD có thể nghiêm trọng
        else:
            logger.info(f"CRD {crd_name} spec (desiredClients) is up-to-date.")

        # --- Lấy Owner Reference từ CRD hiện có ---
        if not crd_uid:
            logger.error(
                f"[Reconcile MODIFIED FAIL] Failed to get UID for existing CRD {crd_name}."
            )
            return
        owner_ref_dict = client.V1OwnerReference(
            api_version=current_crd["apiVersion"],
            kind=current_crd["kind"],
            name=current_crd["metadata"]["name"],
            uid=crd_uid,
            controller=True,
            block_owner_deletion=True,
        )

    except Exception as crd_err:
        logger.error(
            f"[Reconcile MODIFIED FAIL] Error processing CRD {crd_name}: {crd_err}."
        )
        return

    # --- Bước 4: Lấy Deployment hiện có và Patch nếu cần ---
    deployment_name = f"client-{node_name}-deployment"
    try:
        current_deployment = get_deployment(deployment_name)

        if not current_deployment:
            # Lỗi: CRD tồn tại nhưng Deployment không?
            logger.error(
                f"[Reconcile MODIFIED FAIL] Deployment {deployment_name} not found for CRD {crd_name}. Inconsistent state."
            )
            # Thử tạo lại Deployment
            logger.info(f"Attempting to create missing Deployment {deployment_name}...")
            try:
                create_deployment(
                    deployment_name, node_name, desired_replicas, owner_ref_dict
                )
                # Sau khi tạo, lấy lại trạng thái để cập nhật status
                current_deployment = get_deployment(deployment_name)
            except Exception as create_dep_err:
                logger.error(
                    f"[Reconcile MODIFIED FAIL] Failed to recreate missing Deployment {deployment_name}: {create_dep_err}."
                )
                # Không thể tiếp tục nếu không có Deployment
                return
        else:
            # Deployment tồn tại, kiểm tra và patch replicas nếu cần
            current_replicas = current_deployment.spec.replicas
            if current_replicas != desired_replicas:
                logger.info(
                    f"Deployment {deployment_name} replica count mismatch (Current: {current_replicas}, Desired: {desired_replicas}). Patching..."
                )
                try:
                    patch_deployment_replicas(deployment_name, desired_replicas)
                    # Lấy lại deployment sau khi patch để có status mới nhất
                    current_deployment = get_deployment(deployment_name)
                except Exception as patch_dep_err:
                    logger.error(
                        f"[Reconcile MODIFIED WARN] Failed to patch Deployment {deployment_name} replicas: {patch_dep_err}."
                    )
                    # Vẫn tiếp tục để cập nhật status CRD với trạng thái hiện có
            else:
                logger.info(
                    f"Deployment {deployment_name} replica count is correct ({desired_replicas})."
                )
            # TODO: Thêm logic kiểm tra và patch các thay đổi khác nếu cần (image, config, v.v.)

    except Exception as dep_err:
        logger.error(
            f"[Reconcile MODIFIED FAIL] Failed to get or process Deployment {deployment_name}: {dep_err}."
        )
        return

    # --- Bước 5: Cập nhật Status của CRD ---
    active_replicas = 0
    if current_deployment and current_deployment.status:
        active_replicas = current_deployment.status.ready_replicas or 0
        logger.debug(
            f"Deployment {deployment_name} status for CRD update: Replicas={current_deployment.status.replicas}, Ready={active_replicas}"
        )
    else:
        logger.warning(
            f"Could not get status for Deployment {deployment_name} during MODIFIED reconcile."
        )

    if not update_crd_status(crd_name, active_replicas, desired_replicas):
        logger.warning(
            f"[Reconcile MODIFIED WARN] Failed to update status for CRD {crd_name}."
        )

    logger.info(f"[Reconcile MODIFIED END] Successfully processed node {node_name}.")


def handle_deleted_layer1_node(node_name):
    """Xử lý khi Node Layer 1 bị xóa."""
    logger.info(
        f"[Cleanup START] Handling DELETED event for Layer 1 node: {node_name}."
    )
    crd_name = f"clientlayer1-{node_name}"
    if delete_crd(crd_name):
        logger.info(
            f"[Cleanup SUCCESS] Initiated deletion of CRD {crd_name} (and owned Deployment via GC)."
        )
    else:
        logger.error(f"[Cleanup FAIL] Failed to delete CRD {crd_name}.")


def handle_modified_non_layer1_node(node_name):
    """Xử lý khi Node bị sửa đổi và KHÔNG còn là Layer 1 (dọn dẹp)."""
    logger.info(
        f"[Cleanup Check] Handling MODIFIED event for node {node_name} which is NO LONGER Layer 1."
    )
    crd_name = f"clientlayer1-{node_name}"
    try:
        existing_crd = get_crd(crd_name)
        if existing_crd:
            logger.info(
                f"Node {node_name} was previously Layer 1. Cleaning up CRD {crd_name}..."
            )
            if delete_crd(crd_name):
                logger.info(
                    f"[Cleanup SUCCESS] Initiated deletion of CRD {crd_name} (and owned Deployment via GC)."
                )
            else:
                logger.error(
                    f"[Cleanup FAIL] Failed to delete CRD {crd_name} during cleanup check."
                )
        else:
            logger.debug(
                f"No cleanup needed for node {node_name} (CRD {crd_name} not found)."
            )
    except Exception as e:
        logger.error(
            f"[Cleanup FAIL] Error checking/deleting CRD {crd_name} during cleanup check: {e}"
        )
