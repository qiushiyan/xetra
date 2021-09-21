FROM python:3.9-slim-buster

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/workspace/"

# Initializing new working directory
WORKDIR /workspace

# Transferring the code and essential data
COPY xetra_jobs ./xetra_jobs
COPY requirments.txt ./requirments.txt
COPY run.py ./run.py
COPY configs ./configs

RUN pip3 install -r requirments.txt
CMD ["python3", "./run.py"]