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
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    sql=sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        movie['totalNumber'] = row[4]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'movieDatas':data,'Count':len(data),'MysqlTime':timecost})

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
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    sql=sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        movie['totalNumber'] = row[4]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'movieDatas':data,'Count':len(data),'MysqlTime':timecost})

@app.route('/api/userComment',methods=['GET'])
def accordingcomment():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'min' in get_args:
        url=url+'Points>='+get_args['min']
    else:
        url=url+'Points>=0'
    if 'max' in get_args:
        url=url+' and Points<='+get_args['max']
    else:
        url=url+' and Points<=5'   
    print(url)
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    sql=sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        movie['totalNumber'] = row[4]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'movieDatas':data,'Count':len(data),'MysqlTime':timecost})

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
    data1=[]
    for row in data:
        str1=str(row['Movie_id'])
        sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where movie_id='+str1
        
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['Movie_id'] = row[0]
            movie['Title'] = row[1]
            movie['VideoTime'] = row[2]
            movie['Points'] = row[3]
            movie['totalNumber'] = row[4]
            data1.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'movieDatas':data1,'Count':len(data),'MysqlTime':timecost})

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
    data1=[]
    for row in data:
        str1=str(row['Movie_id'])
        sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where product_id="'+str1+'"'
        print(sql)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['Movie_id'] = row[0]
            movie['Title'] = row[1]
            movie['VideoTime'] = row[2]
            movie['Points'] = row[3]
            movie['totalNumber'] = row[4]
            data1.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'movieDatas':data1,'Count':len(data),'MysqlTime':timecost})

@app.route('/api/videoTime',methods=['GET'])
def accordingbyvideoTime():
    time_start=time.time()
    data=[]
    min='0'
    max='10000'
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'min' in get_args:
        min=get_args['min']
    if 'max' in get_args:
        max=get_args['max']    
    
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    url='VideoTime>='+min+' and VideoTime<='+max
    sql=sql+url
    print(url)
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['Movie_id'] = row[0]
        movie['Title'] = row[1]
        movie['VideoTime'] = row[2]
        movie['Points'] = row[3]
        movie['totalNumber'] = row[4]
        data.append(movie)
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'movieDatas':data,'Count':len(data),'MysqlTime':timecost})

@app.route('/api/connection',methods=['GET'])
def accordingconnect():
    time_start=time.time()
    data=[]
    get_args = request.args.to_dict()
    print(get_args)
    url=''
    if 'Director' in get_args:
        url='person_name="'+get_args['Director']+'"'  
        sql = 'select actor_id,cooperation_times from cooperation_relation natural join persons where '
        sql=sql+url+' order by cooperation_times DESC'
        print(url)
        cursor.execute(sql)
        results = cursor.fetchall()
        counter=0
        for row in results:
            counter=counter+1
            movie = {}
            movie['actor_id'] = row[0]
            movie['cooperation_times']=row[1]
            data.append(movie)
            if counter==10:
                break
        data1=[]
        for row in data:
            str1=str(row['actor_id'])
            sql = 'select person_name from persons where person_id="'+str1+'"'
            print(sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                movie = {}
                movie['Actor_name'] = row[0]
                data1.append(movie)   
    else:
        sql = 'select person_id,actor_id,cooperation_times from cooperation_relation natural join persons'
        sql=sql+' order by cooperation_times DESC'
        print(url)
        cursor.execute(sql)
        results = cursor.fetchall()
        counter=0
        for row in results:
            counter=counter+1
            movie = {}
            movie['director_id'] = row[0]
            movie['actor_id'] = row[1]
            movie['cooperation_times']=row[2]
            data.append(movie)
            if counter==10:
                break
        data1=[]
        for row in data:
            movie = {}
            str1=str(row['director_id'])
            str2=str(row['actor_id'])
            sql = 'select person_name from persons where person_id="'+str2+'"'
            print(sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                movie['Actor_name'] = row[0]
            sql = 'select person_name from persons where person_id="'+str1+'"'
            print(sql)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                movie['Director_name'] = row[0]
           
            data1.append(movie)               
    time_end=time.time()
    timecost=time_end-time_start
    return jsonify({'Actor':data1,'Count':len(data),'MysqlTime':timecost})

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=5000,debug=True)
