FROM python:3.9-slim

WORKDIR /app

# Cài đặt dependencies
RUN pip install kubernetes

# Copy file Controller
COPY split_learning_controller.py .

# Chạy Controller
CMD ["python", "split_learning_controller.py"]