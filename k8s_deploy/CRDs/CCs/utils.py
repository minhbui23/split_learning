# utils.py
import math
from app_config import CPU_REQ, RAM_REQ

def calculate_client_count(cpu_alloc, ram_alloc):
    ram_kib = int(ram_alloc.rstrip("Ki"))
    ram_gib = ram_kib / (1024 * 1024)

    cpu_based = int(int(cpu_alloc) / int(CPU_REQ))  
    ram_based = int(ram_gib / int(RAM_REQ))        

    return min(cpu_based, ram_based)
