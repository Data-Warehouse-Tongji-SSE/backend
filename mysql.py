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
    url=''
    if 'Year' in get_args:
        url=url+'Year='+get_args['Year']
    if 'Season' in get_args:
        if url:
            url=url+' '+'and '
        if get_args['Season']==1:
            url=url+"Month<4 and Month>0"
        if get_args['Season']==2:
            url=url+"Month<7 and Month>3"
        if get_args['Season']==3:
            url=url+"Month<10 and Month>6"
        if get_args['Season']==4:
            url=url+'Month<13 and Month>9'
    else: 
        if 'Month' in get_args:
            if url:
                url=url+' and '
            url=url+'Month='+get_args['Month']


    if 'WeekDay' in get_args:
        if url:
            url=url+' and '
        url=url+'WeekDay='+get_args['WeekDay']    
    print(url)
    sql = 'select product_id,movie_title,VideoTime,Points from movies natural join product where '
    sql=sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Data':data,'Count':len(data),'MysqlTime':timecost})

@app.route('/api/title',methods=['GET'])
def accordingtitle():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'Title' in get_args:
        url=url+'movie_title="'+get_args['Title']+'"'
    print(url)
    sql = 'select product_id,movie_title,VideoTime,Points from movies natural join product where '
    sql=sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Data':data,'Count':len(data),'MysqlTime':timecost})

@app.route('/api/userComment',methods=['GET'])
def accordingcomment():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'Points' in get_args:
        url=url+'Points>='+get_args['Points']
    
    print(url)
    sql = 'select product_id,movie_title,VideoTime,Points from movies natural join product where '
    sql=sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Data':data,'Count':len(data),'MysqlTime':timecost})

@app.route('/api/composition',methods=['GET'])
def accordingperson():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'Director' in get_args:
        url='person_name="'+get_args['Director']+'"'
        sql = 'select movie_id from director_relation natural join persons where '
        print(url)
        sql=sql+url
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['Movie_id'] = row[0]
            data.append(movie)
        print(len(data))
    if 'Starring' in get_args:
        url='person_name="'+get_args['Starring']+'"'
        sql = 'select movie_id from starring_relation natural join persons where '
        sql=sql+url
        print(url)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['Movie_id'] = row[0]
            data.append(movie)
        print(len(data))
    if 'Actor' in get_args:
        url='person_name="'+get_args['Actor']+'"'
        sql = 'select movie_id from supporting_relation natural join persons where '
        sql=sql+url
        print(url)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['Movie_id'] = row[0]
            data.append(movie)
        print(len(data))
    if 'Genres' in get_args:
        url='genres_type="'+get_args['Genres']+'"'   
        sql = 'select movie_id from genres_relation natural join genres where '
        sql=sql+url
        print(url)
        cursor.execute(sql)
        print(url)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['Movie_id'] = row[0]
            data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Count':len(data),'MysqlTime':timecost})

@app.route('/api/comment',methods=['GET'])
def accordingbycomment():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'userId' in get_args:
        url=url+'user_id="'+get_args['userId']+'"'
    print(url)
    sql = 'select product_id from comments where '
    sql=sql+url

    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Count':len(data),'MysqlTime':timecost})

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=5000,debug=True)
