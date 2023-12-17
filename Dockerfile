FROM prefecthq/prefect:2.11.5-python3.11

COPY requirements.txt /

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt