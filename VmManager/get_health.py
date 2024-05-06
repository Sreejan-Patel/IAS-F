import psutil

def get_cpu_usage():
    # Get the CPU usage percentage
    return psutil.cpu_percent(interval=1)

def get_memory_free():
    # Get the amount of available memory in megabytes
    mem = psutil.virtual_memory()
    free_memory_mb = mem.available / (1024**2)
    return free_memory_mb

if __name__ == "__main__":
    cpu_usage = get_cpu_usage()
    memory_free = get_memory_free()
    # Output CPU usage and free memory as a comma-separated string
    print(f"{cpu_usage},{memory_free}")
