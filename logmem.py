import os, psutil

def init_log(LOG: str):
    global c, last_rss, last_vms
    with open(LOG, "w") as f:
        pass
    c = 0
    last_rss = None
    last_vms = None

def log_mem_point(label: str, LOG: str):
    global c, last_rss, last_vms

    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    rss_mb = mem_info.rss / (1024 * 1024)
    vms_mb = mem_info.vms / (1024 * 1024)

    rss_delta = rss_mb - last_rss if last_rss is not None else 0
    vms_delta = vms_mb - last_vms if last_vms is not None else 0

    if last_rss is not None and last_vms is not None:
            if rss_delta == 0:
                return 

    if label.startswith("APPENDED"):
        c += 1
        label = f"{c} {label}"
        entry = (f"[{label}] RSS: {rss_mb:.2f} MB ({rss_delta:+.2f}), "
                 f"VMS: {vms_mb:.2f} MB ({vms_delta:+.2f})\n")

    else:
        entry = (f"[{label}] RSS: {rss_mb:.2f} MB ({rss_delta:+.2f}), "
                 f"VMS: {vms_mb:.2f} MB ({vms_delta:+.2f})\n")

    last_rss = rss_mb
    last_vms = vms_mb

    print(entry.strip())
    with open(LOG, "a") as f:
        f.write(entry)