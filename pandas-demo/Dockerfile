FROM python:slim
WORKDIR /app
COPY requirement.txt .
RUN pip install -r requirement.txt
COPY pandas-demo.py .
CMD ["python","pandas-demo.py"]