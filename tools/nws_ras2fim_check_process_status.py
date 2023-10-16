# Purpose:
# Determine if a process with a specific name is running or is idle
# if idle - a flag can be set to kill the process
#
#
# Created by: Andy Carter, PE
# Last revised - 2021.11.04

import argparse
import datetime
import time

import psutil


# -------------------------------------------------
def fn_process_id_by_name(str_process_name):
    """
    Get a list of all the PIDs of a all the running process whose name contains
    the given string str_process_name
    """
    list_process_obj = []
    # Iterate over the all the running process

    for proc in psutil.process_iter():
        try:
            pinfo = proc.as_dict(attrs=["pid", "name", "status", "create_time"])

            # Check if process name contains the given name string.
            if str_process_name.lower() in pinfo["name"].lower():
                list_process_obj.append(pinfo)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return list_process_obj


# -------------------------------------------------
def fn_is_process_running(int_pid, flt_time_check_interval, int_max_interval):
    """
    Keyword arguments:
        int_pid                  - Required  : Windows process ID (int)
        flt_time_check_interval  - Required  : time between each check [seconds] (flt)
        int_max_interval         - Required  : number of time steps to check (int)
    """

    starttime = time.time()
    int_interval = 0

    # int_pid = item.get('pid')
    p = psutil.Process(int_pid)
    flt_pid_cpu = p.cpu_percent()
    b_process_running = False

    while int_interval < int_max_interval:
        int_interval += 1
        time.sleep(flt_time_check_interval - ((time.time() - starttime) % flt_time_check_interval))
        if flt_pid_cpu > 0:
            b_process_running = True
        flt_pid_cpu = p.cpu_percent()

    return b_process_running


# -------------------------------------------------
def fn_check_process_idle(str_process_name, flt_time_step, int_num_time_steps, b_is_kill_idle):
    list_named_processes = fn_process_id_by_name(str_process_name)

    print("Processes Found: " + str(len(list_named_processes)))

    int_current_process = 1

    if len(list_named_processes) > 0:
        for item in list_named_processes:
            int_pid = item.get("pid")
            b_running = fn_is_process_running(int_pid, flt_time_step, int_num_time_steps)

            str_count = str(int_current_process) + " of " + str(len(list_named_processes)) + ": "

            if b_running:
                print(str_count + str(int_pid) + " is running")
            else:
                print(str_count + str(int_pid) + " idle")
                if b_is_kill_idle:
                    p = psutil.Process(int_pid)
                    p.terminate()  # or p.kill()

            int_current_process += 1


# -------------------------------------------------
if __name__ == "__main__":
    flt_start_run = time.time()

    parser = argparse.ArgumentParser(description="=============== DETERMINE PROCESS STATUS =============")

    parser.add_argument(
        "-i",
        dest="str_process_name",
        help="REQUIRED: Name of the process to check Example: Ras.exe",
        required=True,
        metavar="STR",
        type=str,
    )

    parser.add_argument(
        "-t",
        dest="flt_time_step",
        help="OPTIONAL: How often to check process status: Default: 0.5",
        required=False,
        default=0.5,
        metavar="FLT",
        type=float,
    )

    parser.add_argument(
        "-n",
        dest="int_num_time_steps",
        help="OPTIONAL: number of time steps to check: Default: 8",
        required=False,
        default=8,
        metavar="INT",
        type=int,
    )

    parser.add_argument(
        "-k",
        dest="b_is_kill_idle",
        help="OPTIONAL: kill tasks that are determined to be idle: Default: False",
        required=False,
        default=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    str_process_name = args["str_process_name"]
    flt_time_step = args["flt_time_step"]
    int_num_time_steps = args["int_num_time_steps"]
    b_is_kill_idle = args["b_is_kill_idle"]

    print(" ")
    print("+=================================================================+")
    print("|                    DETERMINE PROCESS STATUS                     |")
    print("+-----------------------------------------------------------------+")

    print("  ---(i) NAME OF PROCESS: " + str_process_name)
    print("  ---[t]   Optional: TIME STEP TO CHECK IN SECONDS: " + str(flt_time_step))
    print("  ---[n]   Optional: NUMBER OF TIME STEPS TO CHECK: " + str(int_num_time_steps))
    print("  ---[k]   Optional: KILL IDLE PROCESSES: " + str(b_is_kill_idle))
    print("+-----------------------------------------------------------------+")

    fn_check_process_idle(str_process_name, flt_time_step, int_num_time_steps, b_is_kill_idle)

    flt_end_run = time.time()
    flt_time_pass = (flt_end_run - flt_start_run) // 1
    time_pass = datetime.timedelta(seconds=flt_time_pass)
    print("+-----------------------------------------------------------------+")
    print("Compute Time: " + str(time_pass))
    print("====================================================================")
