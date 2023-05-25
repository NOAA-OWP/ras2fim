
import os

def print_date_time_duration(start_dt, end_dt):
    '''
    Process:
    -------
    Calcuates the diffenence in time between the start and end time
    and prints is as:
    
        Duration: 4 hours 23 mins 15 secs
    
    -------
    Usage:
        from utils.shared_functions import FIM_Helpers as fh
        fh.print_current_date_time()
    
    -------
    Returns:
        Duration as a formatted string
        
    '''
    time_delta = (end_dt - start_dt)
    total_seconds = int(time_delta.total_seconds())

    total_days, rem_seconds = divmod(total_seconds, 60 * 60 * 24)        
    total_hours, rem_seconds = divmod(rem_seconds, 60 * 60)
    total_mins, seconds = divmod(rem_seconds, 60)

    time_fmt = f"{total_hours:02d} hours {total_mins:02d} mins {seconds:02d} secs"
    
    duration_msg = "Duration: " + time_fmt
    print(duration_msg)
    
    return duration_msg

