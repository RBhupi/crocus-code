## Start with a lightweight Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Install system libraries required for building netCDF4 and cftime
RUN apt-get update && apt-get install -y \
    libhdf5-dev \
    libnetcdf-dev \
    libssl-dev \
    libffi-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the Python script and requirements file into the container
COPY ceil_time-adjustment.py ceil_make_daily_files.py requirements.txt ./

# Install Python libraries
RUN pip install --no-cache-dir -r requirements.txt

# Command to run the script
#CMD ["python", "ceil_time-adjustment.py", "-i", "/data/input", "-o", "/data/output", "-p", "*.nc"]

CMD ["/bin/bash"]
