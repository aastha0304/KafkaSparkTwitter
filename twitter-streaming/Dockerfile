FROM python:3
RUN pip install tweepy
RUN pip install kafka-python
RUN pip install python-twitter
ADD twitter_kafkaprod.py /
ENTRYPOINT ["python", "twitter_kafkaprod.py"]
