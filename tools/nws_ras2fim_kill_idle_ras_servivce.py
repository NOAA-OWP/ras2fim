# Purpose:
# Windows process - used to kill idle HEC-RAS processes
# Check to see if a named process is open and idle.  It it is idle over the
# requested number of tries, it closes (kills) the process id
#
# Created by: Andy Carter, PE
# Last revised - 2021.11.05

import time

import psutil

# This is invalid as it likely came from django and we don't load that anymore
# but this class might deprecated, not sure yet.
from service_base_class import SMWinservice


# https://thepythoncorner.com/posts/2018-08-01-how-to-create-a-windows-service-in-python/


# -------------------------------------------------
class class_python_ras2fim_srvice(SMWinservice):
    _svc_name_ = "ras2fimService"
    _svc_display_name_ = "ras2fim - kill idle process"
    _svc_description_ = "** ras2fim - kill idle process"

    # -------------------------------------------------
    def start(self):
        self.isrunning = True

    # -------------------------------------------------
    def stop(self):
        self.isrunning = False

    # -------------------------------------------------
    def main(self):
        str_process_name = "Ras.exe"
        flt_time_step = 0.5
        int_num_time_steps = 8
        b_is_kill_idle = True

        while self.isrunning:
            fn_check_process_idle(str_process_name, flt_time_step, int_num_time_steps, b_is_kill_idle)

            # checks every minute for an idle process
            time.sleep(60)


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

    class_python_ras2fim_srvice.parse_command_line()
