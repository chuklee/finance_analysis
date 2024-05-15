FROM ubuntu:latest
MAINTAINER Olivier Ricou <olivier@ricou.eu.org>

# If you have a web spider getting data, then you can run
# that analyzer every night (therefore cron)

ENV DEBIAN_FRONTEND noninteractive

# Install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
       python3 git make python3-pip python3-setuptools cron bzip2 tzdata python3-venv locales

# Clean up apt cache to reduce image size
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set up locale
RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.UTF-8

# Set timezone
RUN cp /usr/share/zoneinfo/Europe/Paris /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# Create a virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip in the virtual environment
RUN pip install --upgrade pip

# Install dependencies from requirements.txt
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# Add user and set permissions
RUN adduser --disabled-password --gecos "" bourse
RUN chmod 755 /home/bourse

# Switch to user 'bourse'
USER bourse
WORKDIR /home/bourse

# Add application code
ADD apps.tgz .

# Setup the entry point for the container
CMD ["python3", "analyzer.py"]

# Uncomment below if you need to run cron jobs
# USER root
# COPY cronfile /tmp/
# RUN crontab /tmp/cronfile
# CMD service cron start && tail -f /var/log/cron.log