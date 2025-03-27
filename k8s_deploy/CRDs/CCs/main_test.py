# main.py
import time
import logging
from kubernetes import watch
from kubernetes.client.rest import ApiException

try:
    from app_config import (
        logger, v1, 
        LAYER_LABEL_KEY, LAYER_LABEL_VALUE, 
        POD_TEMPLATE_CONFIGMAP_NAME, CRD_GROUP
    )

    from reconciliation import (
        handle_added_layer1_node,
        handle_modified_layer1_node,
        handle_deleted_layer1_node,
        handle_modified_non_layer1_node,
    )
except ImportError as import_err:
    print(
        f"ERROR: Failed to import modules. Check Python path and file structure: {import_err}"
    )
    exit(1)


if not LAYER_LABEL_KEY or not LAYER_LABEL_VALUE:
    logger.critical("Layer label key or value not configured in config.yaml. Exiting.")
    exit(1)


def main():
    logger.info(f"Starting SplitLearningController...")
    logger.info(f"Watching for Nodes with label: {LAYER_LABEL_KEY}={LAYER_LABEL_VALUE}")
    logger.info(
        f"Controller Namespace: {CRD_GROUP}"
    )
    logger.info(
        f"Pod Template CM: {POD_TEMPLATE_CONFIGMAP_NAME}"
    )
    logger.info(f"Log Level: {logging.getLevelName(logger.getEffectiveLevel())}")
    logger.info(f"=========================================")

    w = watch.Watch()
    resource_version = ""

    while True:
        try:
            # Lấy resource version khởi tạo hoặc sau lỗi 410
            if not resource_version:
                try:
                    # Lấy RV từ list node, dùng timeout ngắn
                    logger.debug(
                        "Attempting to get initial resource version for Node watch..."
                    )
                    node_list = v1.list_node(limit=1, _request_timeout=10)
                    if node_list.metadata and node_list.metadata.resource_version:
                        resource_version = node_list.metadata.resource_version
                        logger.info(
                            f"Starting Node watch stream from resourceVersion={resource_version}"
                        )
                    else:
                        logger.warning(
                            "Could not get resource version from initial node list. Retrying..."
                        )
                        time.sleep(5)
                        continue
                except ApiException as e:
                    logger.error(
                        f"API Error getting initial resource version: {e.status} - {e.reason}. Retrying..."
                    )
                    time.sleep(5)
                    continue
                except Exception as e_gen:  # Bắt lỗi timeout hoặc lỗi khác
                    logger.error(
                        f"Error getting initial resource version: {e_gen}. Retrying..."
                    )
                    time.sleep(5)
                    continue

            # Bắt đầu stream sự kiện Node với resource_version đã có
            # Đặt timeout cho stream để tránh bị treo vĩnh viễn và để kiểm tra lại kết nối định kỳ
            logger.debug(f"Establishing Node watch stream with RV={resource_version}")
            # _request_timeout ảnh hưởng đến toàn bộ stream, watch_timeout ảnh hưởng đến từng lần đọc
            for event in w.stream(
                v1.list_node, resource_version=resource_version, timeout_seconds=300):  # Watch 5 phút rồi kết nối lại
                try:
                    event_type = event["type"]
                    node = event["object"]  # Đây là đối tượng V1Node đầy đủ

                    # Kiểm tra xem object có metadata không (quan trọng)
                    if not node.metadata or not node.metadata.name:
                        logger.warning(
                            f"Received event with missing metadata: {event_type}"
                        )
                        continue

                    node_name = node.metadata.name
                    labels = node.metadata.labels or {}
                    spec = node.spec
                    status = node.status  # Lưu ý: status có thể None ban đầu

                    # Cập nhật resource version để lần watch tiếp theo bắt đầu từ đây
                    if node.metadata.resource_version:
                        resource_version = node.metadata.resource_version
                    # Xử lý trường hợp event type là ERROR
                    if event_type == "ERROR":
                        # statusObject là một V1Status dict-like
                        status_object = event.get(
                            "raw_object"
                        )  # raw_object chứa V1Status
                        status_code = status_object.get("code")
                        status_reason = status_object.get("reason")
                        status_message = status_object.get("message")
                        logger.error(
                            f"Watch stream reported an error: Code={status_code}, Reason={status_reason}, Message={status_message}"
                        )
                        if status_code == 410:  # Gone
                            logger.warning(
                                "Resource version too old (410 Gone). Resetting watch."
                            )
                            resource_version = ""  # Reset để lấy RV mới
                            w.stop()  # Dừng stream hiện tại
                            break  # Thoát vòng lặp for event để bắt đầu lại vòng while
                        else:
                            # Lỗi khác, có thể thử lại sau một khoảng thời gian
                            logger.error(
                                f"Unhandled watch error code {status_code}. Retrying connection after delay."
                            )
                            w.stop()
                            time.sleep(15)
                            break  # Thoát for loop, thử kết nối lại

                    # Xác định node có phải Layer 1 không
                    layer_id = labels.get(LAYER_LABEL_KEY)
                    is_layer1 = layer_id == LAYER_LABEL_VALUE

                    logger.debug(
                        f"Event: {event_type} | Node: {node_name} | Is Layer1: {is_layer1} | RV: {resource_version}"
                    )

                    # --- Gọi hàm xử lý tương ứng từ module reconciliation ---
                    if is_layer1:
                        if event_type in ["ADDED"]:
                            # Cần cả spec và status để reconcile
                            if spec and status:
                                handle_added_layer1_node(node_name, spec, status)
                            else:
                                logger.warning(
                                    f"Node {node_name} event {event_type} received but spec or status is missing. Skipping reconcile."
                                )
                        elif event_type in ["MODIFIED"]:
                            # Cần cả spec và status để reconcile
                            if spec and status:
                                handle_modified_layer1_node(node_name, spec, status)
                            else:
                                logger.warning(
                                    f"Node {node_name} event {event_type} received but spec or status is missing. Skipping reconcile."
                                )

                        elif event_type == "DELETED":
                            handle_deleted_layer1_node(node_name)
                    else:  # Not Layer 1
                        # Chỉ cần xử lý khi node *từng là* Layer 1 và giờ không phải nữa
                        # (ví dụ label bị xóa) -> dọn dẹp CRD/Deployment
                        if event_type == "MODIFIED":
                            handle_modified_non_layer1_node(node_name)
                        # Không cần làm gì cho ADDED/DELETED non-layer1 node

                except Exception as e_inner:
                    # Log lỗi xử lý sự kiện cụ thể nhưng không dừng controller
                    node_name_log = node_name if "node_name" in locals() else "UNKNOWN"
                    logger.exception(
                        f"Unhandled error processing event for node {node_name_log} (RV: {resource_version})"
                    )
                    # Nên tiếp tục vòng lặp để xử lý sự kiện tiếp theo

            # Vòng lặp for kết thúc (có thể do timeout hoặc break từ lỗi 410)
            logger.info("Watch stream ended. Re-establishing watch...")
            # Không reset resource_version nếu chỉ là timeout thông thường

        except ApiException as e_outer:
            # Lỗi API ở cấp độ stream (ngoài vòng lặp for)
            if e_outer.status == 410:  # Gone
                logger.warning(
                    "Watch stream failed with 410 Gone. Resetting resource version."
                )
                resource_version = ""  # Reset để lấy RV mới
                time.sleep(1)  # Chờ chút trước khi thử lại
            else:
                logger.exception(
                    f"Kubernetes API error in watch stream: Status {e_outer.status} - {e_outer.reason}. Retrying in 15s."
                )
                time.sleep(15)
        except Exception as e_main:
            # Lỗi nghiêm trọng khác trong vòng lặp chính (ví dụ network error)
            logger.exception("Critical error in main watch loop. Retrying in 30s.")
            time.sleep(30)
        finally:
            # Đảm bảo watch được stop nếu có lỗi xảy ra và thoát khỏi vòng for
            w.stop()


if __name__ == "__main__":
    main()
