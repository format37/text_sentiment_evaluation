# Sentiment analysis
# https://demo.deeppavlov.ai/#/ru/sentiment

from deeppavlov import build_model, configs
import pandas as pd
import datetime
import time
import pymssql
import os

BATCH_SIZE = 2 # 1000

def update_record(server_object, df):

        query = ''

        for index, row in df.iterrows():
                if row.sentiment == 'negative':
                        neg = 1
                        pos = 0
                else:
                        neg = 0
                        pos = 1
                query += "update transcribations set "
                query += "sentiment = '"+row.sentiment+"', "
                query += "sentiment_neg = "+str(neg)+", "
                query += "sentiment_pos = "+str(pos)+" "
                query += "where id = "+str(row.id)+";"
                #print(row.id, row.sentiment)

        cursor = server_object.conn.cursor()
        cursor.execute(query)
        server_object.conn.commit()


def connect_sql():

	return pymssql.connect(
		server=os.environ.get('MSSQL_SERVER', ''),
		user=os.environ.get('MSSQL_LOGIN', ''),
		password=os.environ.get('MSSQL_PASSWORD', ''),
		database='voice_ai'
	)


def main():

        conn = connect_sql()

        #model = build_model(configs.classifiers.rusentiment_bert, download=True)

        line = 0
        #while True:

        query = """
                select top """+str(BATCH_SIZE)+"""
                id,     text, sentiment
                from transcribations
                where sentiment is NULL and text!=''
                order by transcribation_date, start
                """

        df = pd.read_sql(query, conn)

        if len(df) > 0:
                print('solving '+str(len(df))+' records')        
                #df['sentiment'] = model(df.text)
                print(df)
                #update_record(server_object, df)

        else:
                print(datetime.datetime.now(), 'No data. waiting..')
                time.sleep(10)

        print(datetime.datetime.now(), 'next job..')

if __name__ == "__main__":
	main()
