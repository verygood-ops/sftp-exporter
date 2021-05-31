FROM python:3.9-slim-buster
RUN mkdir /app
RUN useradd -m appuser
COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install -r /app/requirements.txt
COPY sftp-exporter.py /app/sftp-exporter.py
RUN chmod a+x /app/sftp-exporter.py
USER appuser
CMD ["/app/sftp-exporter.py"]
