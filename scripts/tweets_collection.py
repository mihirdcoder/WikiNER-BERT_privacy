import MySQLdb
import csv

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



class db_grabber_ne():
    read_table = "messages_en"
    #count_table = "NycNECount"
    #insert_table = "target_tweets"
    insert_table = "z_1_target_tweets"
    #bert_table = "z_2_targetBert"
    bert_table = "z_5_targetBert"
    u_db_name = "twitterGender"
    u_db_name_insert = "twitterBert"

    u_host_name = "localhost"
    u_name = "mparulekar"
    u_pass = None
    u_charset = "utf8mb4"
    u_read_default_file = "~/.my.cnf"

    def __init__(self):
        self.mydb_Bert = MyDB(db_grabber_ne.u_host_name, db_grabber_ne.u_db_name, db_grabber_ne.u_name,
                            db_grabber_ne.u_charset, db_grabber_ne.u_read_default_file, db_grabber_ne.u_pass)

        self.mydb_Bert1 = MyDB(db_grabber_ne.u_host_name, db_grabber_ne.u_db_name_insert, db_grabber_ne.u_name,db_grabber_ne.u_charset, db_grabber_ne.u_read_default_file, db_grabber_ne.u_pass)

        self.mydb_Bert2 = MyDB(db_grabber_ne.u_host_name, db_grabber_ne.u_db_name_insert, db_grabber_ne.u_name,
                                            db_grabber_ne.u_charset, db_grabber_ne.u_read_default_file, db_grabber_ne.u_pass)


    def read_tweets(self,ne):
        q = "SELECT message_id, message from {0} where message like '%{1}%' ".format(self.read_table,ne)
        try:
            print(q,"is executing")
            self.mydb_Bert.query(q)
        except Exception as e:
            print ("read_tweets: ", e)
        print(self.mydb_Bert.db_cur.rowcount)
        return self.mydb_Bert.db_cur.rowcount

    def insert_tweets(self,vals,count):
        #q = "de".format(db_grabber_ne.insert_table)
        print(vals)
        q = "INSERT INTO {0} (message_id,message,ne_count) values({1},%s,{2})".format(self.insert_table,vals[0],count)%('"'+vals[1]+'"')
        #print(q)
        #print("In insert tweets", vals, len(vals), count)
        #print("0", vals[0],"1", vals[1], "2", count, type(vals[0]))
        try:
            self.mydb_Bert1.query(q)
        except Exception as e:
            print("insert tweets error",e)
        #print("insert query output", self.mydb_Bert1.db_cur.fetchmany(size = 50))

    def insertBert1(self,vals):
        ne_index = vals[5]
        message_id = vals[4]
        bert11 = "'"+ vals[0]+"'"
        cls11 ="'"+vals[1]+"'"
        bert12 = "'"+vals[2]+"'"
        cls12 = "'"+vals[3]+"'"
        q = "INSERT INTO {0} (message_id,layer12,cls12,layer11,cls11,ne_count) values(%s,%s,%s,%s,%s,%s)".format(self.bert_table)%(message_id, bert12,cls12,bert11,cls11,ne_index)
        #print(q)
        try:
            self.mydb_Bert2.query(q)
        except Exception as e:
            print ("insert messageEn: ", e)


    def get_one(self):
        try:
            tweet = self.mydb_Bert.db_cur.fetchone()
            return tweet
        except Exception as e:
            print ("get_one: ",e)

    def new_read_table(self,ne_count):
        q = "select message_id, message from z_1_target_tweets where ne_count = "+str(ne_count)+" limit 10000 "
        try:
            print(q, "is executing")
            self.mydb_Bert1.query(q)
        except Exception as e:
            print("new_read_table", e)
        print(self.mydb_Bert1.db_cur.rowcount)
        return  self.mydb_Bert1.db_cur.rowcount

    def new_get_one(self):
        try:
            tweet = self.mydb_Bert1.db_cur.fetchone()
            return tweet
        except Exception as e:
            print ("get_one: ",e)

    def new_get_50(self):
        try:
            tweet = self.mydb_Bert1.db_cur.fetchmany(size = 50)
            print("get_50", len(tweet))
            return tweet
        except Exception as e:
            print ("get_one: ",e)

    def csv_check(self):
        with open('named_entitis1.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            ret = []
            for row in csv_reader:
                try:
                    ans = (row[0], row[1])
                    ret.append(ans)
                except:
                    pass
            return ret




def main():
    ne_db = db_grabber_ne()
    #count = ne_db.read_tweets()
    dic = {}
    #target =  ['Oregon Ducks','U.S.', 'United Kingdom', 'Kitt','North Carolina','Los Angeles','FIS','AnomieBOTUser','Victoria Land', 'Nakhchivan', 'Carlisi', 'National Public Radio','Davos','Emily Nishikawa','Ostrov']
    ret = ne_db.csv_check()
    #target1 = ["United States","Oregon Ducks"]
    for ne_c , ne in enumerate(ret):
        if ne_c > 23:
            try:
                print(ne_c, ne)
                new_count , named_entity = ne[0],ne[1]
                count = ne_db.read_tweets(named_entity)
                for i in range(0, count):
                    tweet = ne_db.get_one()
                    ne_db.insert_tweets(tweet, new_count)
                    dic[i] = "Mihir__"+str(i)
            except Exception as e:
                print("Not found",e, ne)


'''
def main():
    ne_db = db_grabber_ne()
    ret = ne_db.csv_check()
    print(ret)
'''

if __name__ == "__main__":
    main()