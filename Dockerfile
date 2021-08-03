

FROM python:3.8

ADD cloud_publish_container.py /cloud_publish_container.py
ADD cloudcredentials.json /cloudcredentials.json




COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "cloud_publish_container.py"]
