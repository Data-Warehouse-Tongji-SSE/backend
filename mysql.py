from flask import Flask,jsonify,request
from flask.json import tojson_filter
import pymysql
import time

db = pymysql.connect(
    host='8.133.173.118',
    port=13306,
    user='root',
    passwd='moreonenight24680no',
    db='datawarehouse',
    )# 记得修改数据库地址
cursor = db.cursor()

app = Flask(__name__)

@app.route('/<id>',methods=['GET'])
def mysqlTest(id):
    time_start=time.time()

    print(id)
    sql = 'select movie_id,movie_title from movies where movie_id='
    sql=sql+id
    cursor.execute(sql)
    results = cursor.fetchall()
    title=""
    for row in results:
        print(row[0])
        title=row[1]
    print(title)
    time_end=time.time()
    timecost=time_end-time_start
    print(timecost)
    return jsonify({'title':title,'MysqlTime':timecost})

@app.route('/api/time',methods=['GET'])
def accordingtime():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    sql = 'select porduct_id,movie_title,VideoTime,Points from movies natural join product where '
    sql=sql
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Data':data,'count':len(data),'MysqlTime':timecost})


if __name__ == '__main__':
    app.run(host='127.0.0.1',port=5000,debug=True)
