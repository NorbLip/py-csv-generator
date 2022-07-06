# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY . /app

# Make it interactive
ENTRYPOINT [ "/bin/bash" ]

# docker build -t python-image .
# docker run -it python-image
# docker run -it -v sourcepath:destpath python-image
# docker run -it -v C:\GitHub\py-csv-generator\:/app python-image
