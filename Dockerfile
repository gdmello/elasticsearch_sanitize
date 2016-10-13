FROM dev-docker.points.com:80/jsonymous
RUN apt-get update && \
    apt-get install -y python python-pip
ADD ["sanitize/", "/sanitize"]
ADD ["requirements.txt", "/sanitize/"]
RUN pip install -r /sanitize/requirements.txt
RUN mkdir /sanitize/logs
VOLUME /sanitize/logs
WORKDIR /sanitize
CMD ["/usr/bin/python", "/sanitize/main.py", "-h"]