
# Time adjustment in Ceil netCDF files by correct timestamps based on file modification times.
# Run: ` python ceil_time-adjustment.py -i /path/to/input_dir -o /path/to/output_dir -p "*.nc"`

import globus_compute_sdk

def gc_timecorrect_cl61(input_dir, output_dir, file_pattern="*.nc", delay=0):
    import os
    import shutil
    import datetime
    from netCDF4 import Dataset, num2date
    import glob
    import re
    import logging

    def get_modification_time(file_path, latency):
        try:
            mod_time_epoch = os.path.getmtime(file_path) - latency
            return datetime.datetime.fromtimestamp(mod_time_epoch, datetime.UTC)
        except Exception as e:
            logging.error(f"Error getting modification time for {file_path}: {e}")
            return None

    def new_file_name(original_file_path, mod_time):
        base_name = os.path.basename(original_file_path)
        new_date_time = mod_time.strftime('%Y%m%d_%H%M%S')
        new_base_name = re.sub(r'\d{8}_\d{6}', new_date_time, base_name)
        return os.path.join(output_dir, new_base_name)



    def adjust_time_variable(time_var, mod_time, midnight):
        """
        Use file modification time.
        """
        try:
            times = num2date(time_var[:], units=time_var.units)  # Convert num times to pyhton datetime objects
            # in this version we are getting interval from the file (No assumptions).
            delta_seconds = [(t - times[0]).total_seconds() for t in times]  
            mod_time = mod_time.replace(tzinfo=None)

            # This end of the last observations time should be align the file's modification time.  
            # i am using delta_Seconds because vaisla files last observation time is not same as file name.
            total_interval = (times[-1] - times[0]).total_seconds()
            #time_interval = datetime.timedelta(seconds=delta_seconds[0])
            seconds_since_midnight = (mod_time - midnight).total_seconds() - total_interval

            adjusted_times = [seconds_since_midnight + delta for delta in delta_seconds] 
            
            time_var[:] = adjusted_times  # change nc time to new times
            time_var.units = f'seconds since {midnight.strftime("%Y-%m-%d 00:00:00")}'
        except Exception as e:
            logging.error(f"Error adjusting time variable: {e}")



    def adjust_time_axis(nc_file, mod_time):
        """
        Adjusts time directly in the file.
        """
        midnight = datetime.datetime(mod_time.year, mod_time.month, mod_time.day)
        
        # Adjust the main time variable
        if 'time' in nc_file.variables:
            adjust_time_variable(nc_file.variables['time'], mod_time, midnight)
        
        # and noe the groups
        for group_name in ['monitoring', 'status']:
            if group_name in nc_file.groups:
                group = nc_file.groups[group_name]
                if 'time' in group.variables:
                    adjust_time_variable(group.variables['time'], mod_time, midnight)
                else:
                    logging.warning(f"Time variable not found in {group_name} group.")
            else:
                logging.info(f"Group '{group_name}' not found in the file.")



    def process_file(original_file_path, latency):
        mod_time = get_modification_time(original_file_path, latency)
        if mod_time is None:
            return
        
        new_file_path = new_file_name(original_file_path, mod_time)
        try:
            shutil.copy(original_file_path, new_file_path)
            with Dataset(new_file_path, 'r+') as nc_file:
                adjust_time_axis(nc_file, mod_time)
            logging.info(f'Processed {original_file_path} ---> {new_file_path}')
        except Exception as e:
            logging.error(f"Error processing file {original_file_path}: {e}")


    from dataclasses import dataclass

    @dataclass
    class ArgsClass:
        input_dir: str
        output_dir: str
        file_pattern: str
        delay:int
    args = ArgsClass(input_dir, output_dir, file_pattern, delay)

    # make output directory if not exit
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # create logfile
    logfile = os.path.join(
        args.output_dir, datetime.datetime.now().strftime("log_ceil-time-adjustment_%Y-%m-%d_%H-%M-%S.txt")
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(logfile), logging.StreamHandler()],
    )

    for file_path in glob.glob(os.path.join(args.input_dir, args.file_pattern)):
        process_file(file_path, latency=args.delay)

    return file_path



# Creating Globus Compute client
gcc = globus_compute_sdk.Client()

# Register the function
COMPUTE_FUNCTION_ID = gcc.register_function(gc_timecorrect_cl61)

# Write function UUID in a file
uuid_file_name = "gc_timecorrect_ceil_uuid.txt"
with open(uuid_file_name, "w") as file:
    file.write(COMPUTE_FUNCTION_ID)
    file.write("\n")
file.close()

# End of script
print("Function registered with UUID -", COMPUTE_FUNCTION_ID)
print("The UUID is stored in " + uuid_file_name + ".")
print("")