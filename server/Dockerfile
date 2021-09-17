FROM deeppavlov/base-gpu:latest
WORKDIR /service
COPY requirements.txt /service
RUN pip3 install -r requirements.txt --no-cache-dir
RUN python3 -m deeppavlov install squad_bert
#RUN python3 -m deeppavlov install config_file
#RUN pip3 install med2image
COPY server.py /service
#RUN wget raw.githubusercontent.com/deepmipt/DeepPavlov/master/deeppavlov/configs/classifiers/rusentiment_bert.json
ENTRYPOINT ["python3"]
CMD ["server.py"]