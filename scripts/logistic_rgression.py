import pandas as pd
import numpy as np
import json
import csv
from ast import literal_eval
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
import MySQLdb


class MyDB():
    def __init__(self, u_host, u_db, u_user, u_charset, u_read_default_file, u_pass = None):
        self.db_connection = None
        if(u_pass == None):
            self.db_connection = MySQLdb.connect(host=u_host, db= u_db, user = u_user, charset=u_charset, read_default_file=u_read_default_file)
        else:
            self.db_connection = MySQLdb.connect(host=u_host, db= u_db, user = u_user, passwd = u_pass, charset=u_charset, read_default_file=u_read_default_file)
        self.db_cur = self.db_connection.cursor()
        self.db_connection.autocommit(True)

    def query(self, query, params = None):
        if(params == None):
            return self.db_cur.execute(query)
        else:
            return self.db_cur.execute(query, params)

    def __del__(self):
        self.db_connection.close()


class db_wrapper_ne:

    NE_table = "NycTweetsNE1"
    read_table = "NycUserDataFinal"
    count_table = "NycNECount"
    word_table = "NycWordNE"
    u_db_name = "twitterBert"
    u_host_name = "localhost"
    u_name = "mparulekar"
    u_pass = None
    u_charset = "utf8mb4"
    u_read_default_file = "~/.my.cnf"

    def __init__(self):
        self.mydb = MyDB(db_wrapper_ne.u_host_name, db_wrapper_ne.u_db_name, db_wrapper_ne.u_name,
                            db_wrapper_ne.u_charset, db_wrapper_ne.u_read_default_file, db_wrapper_ne.u_pass)
        self.mydb_create = MyDB(db_wrapper_ne.u_host_name, db_wrapper_ne.u_db_name, db_wrapper_ne.u_name,
                            db_wrapper_ne.u_charset, db_wrapper_ne.u_read_default_file, db_wrapper_ne.u_pass)


    def get_one(self):
      try:
        tweet = self.mydb.db_cur.fetchone()
        return tweet
      except Exception as e:
        print ("get_one: ",e)


    def get_top_named_entity(self,):
      q = "SELECT * from NycNECount order by count desc ;"
      try:
        self.mydb.query(q)
      except Exception as e:
        print("read_tweets: ", e)
      return self.mydb.db_cur.rowcount

    def createBert1(self,entity):
      q = "create table newBert select message_id,layer11,cls11 from NycBert1 where message_id in (select message_id from NycWordNE where concat(person,location,organization) like '%{0}')".format(entity)
      print("createBert1",q )
      try:
        self.mydb_create.query(q)
      except Exception as e:
        print ("read_tweets: ", e)
      print("create Bert 1: ",self.mydb.db_cur.rowcount)
      return self.mydb.db_cur.rowcount

    def createBert0(self,ne,count):
      q = "create table zeroBert select message_id,layer11,cls11 from z_2_targetBert where ne_count != "+str(count)+" limit "+ str(count)
      #print("etity: ", entity," cnt" ,cnt)
      try:
        self.mydb_create.query(q)
      except Exception as e:
        print ("read_tweets: ", e)
      return self.mydb_create.db_cur.rowcount

    def createBert2(self,count):
        q = "create table check_mihir_target select message_id,layer11,cls11 from z_2_targetBert where ne_count =  " + str(count)
        print("createBert1",q )
        try:
            self.mydb_create.query(q)
        except Exception as e:
            print ("read_tweets: ", e)
            return 0
        print("create Bert 2: ",self.mydb_create.db_cur.rowcount)
        return self.mydb_create.db_cur.rowcount


    def drop_table(self, table_name):
      q = "drop table {}".format(table_name)
      try:
        self.mydb_create.query(q)
      except Exception as e:
        print ("read_tweets: ", e)
      print("dropped: ",table_name)


    def read_data_frame(self,table_name):
      q ='select * from {};'.format(table_name)
      try:
        df = pd.read_sql(q, con=self.mydb.db_connection)
      except Exception as e:
        print ("read_tweets: ", e)
      return df

    def insert_output(self,en,count, acc, fscore, mat):
      print("In insert_output")
      with open("code/named_entitis1.csv") as csv_file:
        csv_reader = csv.reader(csv_file , delimiter = ",")
        dic = {}
        for row in csv_reader:
          try:
            dic[row[0]] = row[1]
          except:
            pass
      print("In insert_output function")
      print("Insert print ", dic,dic[str(en)])
      word = "'"+dic[str(en)]+"'"
      mat = "'"+json.dumps(mat.tolist())+"'"
      q = 'insert into z_1_outout_cls_1 (word, count, accuracy, fscore, mat) values ({},{},{},{},{})'.format(word, count, acc,fscore,mat)
      print(q)
      try:
        self.mydb_create.query(q)
      except Exception as e:
        print ("insert_output: ", e)
      return self.mydb.db_cur.rowcount


def get_input_dataframe(df,df_zero):
  df["label"] = 1
  df_zero["label"] = 0
  df = pd.concat([df,df_zero])
  df["layer11"] = df.layer11.apply(lambda s: list(literal_eval(s)))
  df["cls11"] = df.cls11.apply(lambda s: list(literal_eval(s)))
  df = df.sample(frac=1).reset_index(drop=True)
  return df

def get_conf_matrix(df):
  X_train, X_test, y_train, y_test = train_test_split( df.cls11, df.label, test_size=0.4, random_state=42)
  X_train = np.array(list(map(lambda x: np.array(x), X_train)))
  X_test = np.array(list(map(lambda x: np.array(x), X_test)))
  y_train = np.array(list(map(lambda x: np.array(x), y_train)))
  y_test = np.array(list(map(lambda x: np.array(x), y_test)))
  print(np.unique(X_train),np.unique(y_train))
  print(df.shape)
  print(type(X_train), type(y_train))
  try:
    clf = LogisticRegression(random_state=0, solver='liblinear', penalty = "l1").fit(X_train, y_train)
  except Exception as e:
    print("Skipped 1 ")
    return [0,0,np.arange(0)]
  pred = clf.predict(X_test)
  acc = clf.score(X_test,y_test)
  fscore = f1_score(y_test, pred, average='macro')
  conf_mat = confusion_matrix(y_test, pred)
  print("Accuracy: ",acc)
  print("F1 score: ",fscore)
  print(conf_mat)
  return [acc, fscore, conf_mat]

def get_conf_matrix_layer11(df):
  X_train, X_test, y_train, y_test = train_test_split( df.layer11, df.label, test_size=0.40, random_state=42)
  X_train = np.array(list(map(lambda x: np.array(x), X_train)))
  X_test = np.array(list(map(lambda x: np.array(x), X_test)))
  y_train = np.array(list(map(lambda x: np.array(x), y_train)))
  y_test = np.array(list(map(lambda x: np.array(x), y_test)))
  print(np.unique(X_train),np.unique(y_train))
  print(df.shape)
  print(type(X_train), type(y_train))
  try:
    clf = LogisticRegression(random_state=0, solver='liblinear', penalty = "l1").fit(X_train, y_train)
  except Exception as e:
    print("Skipped 1 ")
    return [0,0,np.arange(0)]
  pred = clf.predict(X_test)
  acc = clf.score(X_test,y_test)
  fscore = f1_score(y_test, pred, average='macro')
  conf_mat = confusion_matrix(y_test, pred)

  print("Accuracy: ",acc)
  print("F1 score: ",fscore)
  print(conf_mat)
  return [acc, fscore, conf_mat]



if __name__ == '__main__':
    db = db_wrapper_ne()


    for i in range(0, 50):
        try:
            rc_count = db.createBert2(i)
            db.createBert0(i, 5*rc_count)
            df_label = db.read_data_frame("check_mihir_target")
            df_zero = db.read_data_frame("zeroBert")
            df = get_input_dataframe(df_label, df_zero)
            #lr_output = get_conf_matrix_layer11(df)
            lr_output = get_conf_matrix(df)
            print("insert output is called")
            db.insert_output(i,rc_count,lr_output[0],lr_output[1],lr_output[2])
            db.drop_table("check_mihir_target")
            db.drop_table("zeroBert")
            print(i)
        except Exception as e:
            db.drop_table("check_mihir_target")
            db.drop_table("zeroBert")
            print("Failed at ",i,e)







    '''
  db = db_wrapper_ne()
  ne_count = db.get_top_named_entity()


  for i in range(0,ne_count):
    print(i)
    ne = db.get_one()
    rc_count = db.createBert1(ne[0])
    print("rc_count: ", rc_count)
    try:
      db.createBert0(ne[0],ne[1]*5)
      df_label = db.read_data_frame("newBert")
      df_zero = db.read_data_frame("zeroBert")

      df = get_input_dataframe(df_label, df_zero)

      #lr_output = get_conf_matrix(df)
      lr_output = get_conf_matrix_layer11(df)
      db.insert_output(ne[0], ne[1], lr_output[0], lr_output[1], lr_output[2])
      print(df_label.shape)
      print(df_zero.shape)
      db.drop_table("newBert")
      db.drop_table("zeroBert")
    except Exception as e:
      print("Failed at ",ne[0])
   '''
