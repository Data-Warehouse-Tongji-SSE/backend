from flask import Flask, jsonify, request
import pymysql
from py2neo import Graph, Node, Relationship
import time as Time
from time import sleep

from flask.json import tojson_filter

from impala.dbapi import connect

from gevent import pywsgi

# 连接neo4j
graph = Graph('bolt://{your_neo4j_ip}:{your_neo4j_port}', username='neo4j',
              password='{your_neo4j_password}')  # 用户名只能是neo4j，密码是自己定的密码

# 连接Hive；Hive默认没有用户名和密码，输入任意非空用户名和密码均可连接
conn = connect(host='{your_hive_ip}', port=int("{your_hive_port}"), database='default',
               user='{your_hive_user}', password='{your_hive_password}', auth_mechanism='PLAIN')
cur = conn.cursor()

# 连接mysql
db = pymysql.connect(
    host='{your_mysql_ip}',
    port=int("{your_mysql_port}"),
    user='{your_mysql_user}',
    passwd='{your_mysql_password}',
    db='{your_mysql_database}',
)
cursor = db.cursor()

app = Flask(__name__)

# 根据时间查询电影


@app.route('/api/time', methods=['GET'])
def getMoviesByTime():
    # 某年  某年某季度  某年某月  周几
    get_args = request.args.to_dict()
    # Hive
    sql = 'select id,title,videoTime,points,totalNumber from movie where'
    n = 0
    if 'Year' in get_args.keys():
        year = get_args['Year']
        if n != 0:
            sql = sql+' and'
        sql = sql+' year = '+year
        n = n+1
    if 'Month' in get_args.keys():
        month = get_args['Month']
        if n != 0:
            sql = sql+' and'
        sql = sql+' month ='+month
        n = n+1
    if 'Season' in get_args.keys():
        season = get_args['Season']
        month1 = (int(season)-1)*3
        month2 = int(season)*3
        if n != 0:
            sql = sql+' and'
        sql = sql+' month > '+str(month1)+' and month <= '+str(month2)
        n = n+1
    if 'WeekDay' in get_args.keys():
        weekday = get_args['WeekDay']
        if n != 0:
            sql = sql + ' and'
        sql = sql+' weekday = '+weekday
        n = n+1
    begin = Time.time()
    cur.execute(sql)
    end = Time.time()
    results = cur.fetchall()
    hiveTime = end-begin

    # mysql

    data1 = []
    print(get_args)
    url = ''
    if 'Year' in get_args:
        url = url+'Year='+get_args['Year']
    if 'Season' in get_args:
        if url:
            url = url+' '+'and '
        if get_args['Season'] == '1':
            url = url+"Month<4 and Month>0"
        if get_args['Season'] == '2':
            url = url+"Month<7 and Month>3"
        if get_args['Season'] == '3':
            url = url+"Month<10 and Month>6"
        if get_args['Season'] == '4':
            url = url+'Month<13 and Month>9'
    else:
        if 'Month' in get_args:
            if url:
                url = url+' and '
            url = url+'Month='+get_args['Month']
    if 'WeekDay' in get_args:
        if url:
            url = url+' and '
        url = url+'WeekDay='+get_args['WeekDay']
    print(url)
    time_start = Time.time()
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    sql = sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    time_end = Time.time()
    for row in results:
        movie = {}
        movie['id'] = row[0]
        movie['title'] = row[1]
        movie['videoTime'] = row[2]
        movie['points'] = row[3]
        movie['totalNumber'] = row[4]
        data1.append(movie)

    timecost = time_end-time_start
    # Neo4j
    season = None
    if 'Season' in get_args.keys():
        n = int(get_args['Season'])
        season = [n*3-2, n*3-1, n*3]
        del get_args['Season']
    data = []
    start = Time.time()
    if season is None:
        nodes = graph.nodes.match('Movie', **get_args)
    else:
        match_str = f"(_.Month =~ '{season[0]}' or _.Month =~ '{season[1]}' or _.Month =~ '{season[2]}')"
        # match_str = f"_.Month =~ '{season[0]}'"
        nodes = graph.nodes.match('Movie', **get_args).where(match_str)
    end = Time.time()
    for node in nodes:
        dic = dict(node)
        movie = {}
        movie['id'] = dic['id']
        movie['title'] = dic['Title']
        movie['videoTime'] = dic['VideoTime']
        movie['points'] = dic['Points']
        movie['totalNumber'] = dic['totalNumber']

        data.append(movie)

    time = (end - start)

    return jsonify({
        'data': {
            'count': len(data1),
            'Neo4jTime': time*1000,
            'HiveTime': hiveTime*1000,
            'MySqlTime': timecost*1000,
            'movieDatas': data1
        }
    })

# 根据电影名称查询电影


@app.route('/api/title', methods=['GET'])
def getMoviesByTitle():
    get_args = request.args.to_dict()
    # Neo4j
    data = []
    start = Time.time()
    nodes = graph.nodes.match('Movie', **get_args)
    # 加入查询版本
    end = Time.time()
    for node in nodes:
        dic = dict(node)
        movie = {}
        movie['id'] = dic['id']
        movie['title'] = dic['Title']
        movie['videoTime'] = dic['VideoTime']
        movie['points'] = dic['Points']
        movie['totalNumber'] = dic['totalNumber']
        data.append(movie)

    time = end - start

    # Hive
    Title = ' '
    if 'Title' in get_args.keys():
        Title = get_args['Title']
    sql = 'select id,title,videoTime,points,totalNumber from movie where title = "' + Title + '"'
    begin = Time.time()
    cur.execute(sql)
    end = Time.time()
    hiveTime = end-begin
    results = cur.fetchall()

    # mysql

    data1 = []
    data2 = []
    print(get_args)
    url = ''
    if 'Title' in get_args:
        url = url+'movie_title="'+get_args['Title']+'"'
        time_start = Time.time()
        sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
        sql = sql+url
        cursor.execute(sql)
        results = cursor.fetchall()
        time_end = Time.time()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            sql = 'select attitude from comments where product_id="'+row[0]+'"'
            cursor.execute(sql)
            results1 = cursor.fetchall()
            movie['positiveComments'] = 0
            movie['neutralComments'] = 0
            movie['negativeComments'] = 0
            for row1 in results1:
                if row1[0] == 1:
                    movie['positiveComments'] = movie['positiveComments']+1
                if row1[0] == 0:
                    movie['neutralComments'] = movie['neutralComments']+1
                if row1[0] == -1:
                    movie['negativeComments'] = movie['negativeComments']+1
            movie['title'] = row[1]
            movie['videoTime'] = row[2]
            movie['points'] = row[3]
            movie['totalNumber'] = row[4]
            data1.append(movie)

    timecost = time_end-time_start
    return jsonify({
        'data': {
            'count': len(data1),
            'Neo4jTime': time*1000,
            'HiveTime': hiveTime*1000,
            'MySqlTime': timecost*1000,
            'movieDatas': data1
        }
    })

# 根据用户评论查询电影


@app.route('/api/userComment', methods=['GET'])
def getMoviesByUserComment():
    get_args = request.args.to_dict()
    data = []
    # Neo4j
    start = Time.time()
    if 'min' not in get_args.keys():
        nodes = graph.nodes.match('Movie').where(
            f"_.Points <= '{get_args['max']}'")
    elif 'max' not in get_args.keys():
        nodes = graph.nodes.match('Movie').where(
            f"_.Points >= '{get_args['min']}'")
    else:
        nodes = graph.nodes.match('Movie').where(
            f"_.Points >= '{get_args['min']}' and _.Points <= '{get_args['max']}'")
    end = Time.time()

    for node in nodes:
        dic = dict(node)
        movie = {}
        movie['id'] = dic['id']
        movie['title'] = dic['Title']
        movie['videoTime'] = dic['VideoTime']
        movie['points'] = dic['Points']
        movie['totalNumber'] = dic['totalNumber']
        data.append(movie)

    time = end - start

    # Hive
    min = 0
    max = 0
    if 'min' in get_args.keys():
        min = get_args['min']
    if 'max' in get_args.keys():
        max = get_args['max']
    sql = 'select distinct id,title,videoTime,points,totalNumber from movie where points >= ' + \
        str(min) + ' and points <=' + str(max)
    begin = Time.time()
    cur.execute(sql)
    results = cur.fetchall()
    end = Time.time()
    hiveTime = end-begin

    # mysql

    data1 = []
    print(get_args)
    url = ''
    if 'min' in get_args:
        url = url+'Points>='+get_args['min']
    else:
        url = url+'Points>=0'
    if 'max' in get_args:
        url = url+' and Points<='+get_args['max']
    else:
        url = url+' and Points<=5'
    time_start = Time.time()
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    sql = sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    time_end = Time.time()
    for row in results:
        movie = {}
        movie['id'] = row[0]
        movie['title'] = row[1]
        movie['videoTime'] = row[2]
        movie['points'] = row[3]
        movie['totalNumber'] = row[4]
        data1.append(movie)
    timecost = time_end-time_start
    return jsonify({
        'data': {
            'count': len(data1),
            'Neo4jTime': time*1000,
            'HiveTime': hiveTime*1000,
            'MySqlTime': timecost*1000,
            'movieDatas': data1
        }
    })

# 根据导演、演员、电影类别查询电影


@app.route('/api/composition', methods=['GET'])
def getMoviesByComposition():
    get_args = request.args.to_dict()
    data = []
    # Neo4j
    match_str = ""
    if 'Director' in get_args.keys():
        match_str = match_str + \
            "match (:Person {name:'" + \
            get_args['Director']+"'})-[d:DIRECTED]->(m:Movie) "
    if 'Starring' in get_args.keys():
        match_str = match_str + \
            "match (:Person {name:'" + \
            get_args['Starring']+"'})-[s:STARRED]->(m:Movie) "
    if 'Actor' in get_args.keys():
        match_str = match_str + \
            "match (:Person {name:'" + \
            get_args['Actor']+"'})-[x:SUPPORTED]->(m:Movie) "
    if 'Genres' in get_args.keys():
        match_str = match_str + \
            "match (m:Movie)-[i:IS_TYPE]->(:Genres {type:'" + \
            get_args['Genres']+"'}) "
    match_str = match_str + 'return m'
    start = Time.time()
    nodes = graph.run(match_str)
    end = Time.time()

    for node in nodes:
        dic = dict(node)['m']
        movie = {}
        movie['id'] = dic['id']
        movie['title'] = dic['Title']
        movie['videoTime'] = dic['VideoTime']
        movie['points'] = dic['Points']
        movie['totalNumber'] = dic['totalNumber']
        data.append(movie)

    time = end - start

    table = ' movie'
    query = ''
    if 'Director' in get_args.keys():
        director = get_args['Director']
        table = table+' join director on movie.id=director.id'
        if query != '':
            query = query+' and'
        query = query+' directorName = "'+director+'"'
    if 'Starring' in get_args.keys():
        starring = get_args['Starring']
        table = table+' join starring on movie.id=starring.id'
        if query != '':
            query = query+' and'
        query = query+' starringName = "'+starring+'"'
    if 'Actor' in get_args.keys():
        actor = get_args['Actor']
        table = table+' join actor on movie.id=actor.id'
        if query != '':
            query = query+' and'
        query = query+' actorName = "'+actor+'"'
    if 'Genres' in get_args.keys():
        genres = get_args['Genres']
        table = table+' join genres on movie.id=genres.id'
        if query != '':
            query = query+' and'
        query = query+' genresName = "'+genres+'"'
    sql = 'select distinct movie.id,title,videoTime,points,totalNumber from' + \
        table + ' where' + query
    begin = Time.time()
    cur.execute(sql)
    end = Time.time()
    hiveTime = end-begin
    results = cur.fetchall()

    # mysql

    data2 = []
    data4 = []
    data5 = []
    url = ''
    if 'Director' in get_args:
        url = 'person_name="'+get_args['Director']+'"'
        sql = 'select movie_id from director_relation natural join persons where '
        sql = sql+url
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            data2.append(movie)
    if 'Starring' in get_args:
        url = 'person_name="'+get_args['Starring']+'"'
        sql = 'select movie_id from starring_relation natural join persons where '
        sql = sql+url
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            data2.append(movie)
    if 'Actor' in get_args:
        url = 'person_name="'+get_args['Actor']+'"'
        sql = 'select movie_id from supporting_relation natural join persons where '
        sql = sql+url
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            data2.append(movie)
    if 'Genres' in get_args:
        url = 'genres_type="'+get_args['Genres']+'"'
        sql = 'select movie_id from genres_relation natural join genres where '
        sql = sql+url
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            data2.append(movie)
    data1 = []
    time_start = Time.time()
    for row in data2:
        str1 = str(row['id'])
        sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where movie_id='+str1

        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            movie['title'] = row[1]
            movie['videoTime'] = row[2]
            movie['points'] = row[3]
            movie['totalNumber'] = row[4]
            data1.append(movie)
    time_end = Time.time()
    timecost = time_end-time_start
    return jsonify({
        'data': {
            'count': len(data),
            'Neo4jTime': time*1000,
            'HiveTime': hiveTime*1000,
            'MySqlTime': timecost*1000,
            'movieDatas': data
        }
    })

# 根据用户ID查询评论过的电影


@app.route('/api/comment', methods=['GET'])
def getMoviesByUserId():
    get_args = request.args.to_dict()
    data = []
    # Neo4j
    start = Time.time()
    user = graph.nodes.match('User', user_id=f'{get_args["userId"]}').first()
    relations = graph.match((user,), r_type='COMMENTED')
    end = Time.time()

    for relation in relations:
        node = relation.end_node
        dic = dict(node)
        movie = {}
        movie['id'] = dic['id']
        movie['title'] = dic['Title']
        movie['videoTime'] = dic['VideoTime']
        movie['points'] = dic['Points']
        movie['totalNumber'] = dic['totalNumber']
        data.append(movie)

    time = end - start

    # Hive
    userId = ' '
    if 'userId' in get_args.keys():
        userId = get_args['userId']
    sql = 'select distinct movie.id,title,videoTime,points,totalNumber from movie join comment on movie.id=comment.id where userId = "' + userId + '"'
    begin = Time.time()
    cur.execute(sql)
    end = Time.time()
    hiveTime = end-begin
    results = cur.fetchall()

    # mysql
    time_start = Time.time()
    data2 = []
    print(get_args)
    url = ''
    if 'userId' in get_args:
        url = url+'user_id="'+get_args['userId']+'"'
    print(url)
    sql = 'select product_id from comments where '
    sql = sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        movie = {}
        movie['id'] = row[0]
        data2.append(movie)
    data1 = []
    for row in data2:
        str1 = str(row['id'])
        sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where product_id="'+str1+'"'
        print(sql)
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            movie = {}
            movie['id'] = row[0]
            movie['title'] = row[1]
            movie['videoTime'] = row[2]
            movie['points'] = row[3]
            movie['totalNumber'] = row[4]
            data1.append(movie)
    time_end = Time.time()
    timecost = time_end-time_start
    return jsonify({
        'data': {
            'count': len(data1),
            'Neo4jTime': time*1000,
            'HiveTime': hiveTime*1000,
            'MySqlTime': timecost*1000,
            'movieDatas': data1
        }
    })

# 根据时长查询电影


@app.route('/api/videoTime', methods=['GET'])
def getMoviesByVideoTime():
    get_args = request.args.to_dict()
    data = []
    # Neo4j
    start = Time.time()
    if 'min' not in get_args.keys():
        nodes = graph.nodes.match('Movie').where(
            f"_.VideoTime <= '{get_args['max']}'")
    elif 'max' not in get_args.keys():
        nodes = graph.nodes.match('Movie').where(
            f"_.VideoTime >= '{get_args['min']}'")
    else:
        nodes = graph.nodes.match('Movie').where(
            f"_.VideoTime >= '{get_args['min']}' and _.VideoTime <= '{get_args['max']}'")
    sleep(1)
    end = Time.time()

    for node in nodes:
        dic = dict(node)
        movie = {}
        movie['id'] = dic['id']
        movie['title'] = dic['Title']
        movie['videoTime'] = dic['VideoTime']
        movie['points'] = dic['Points']
        movie['totalNumber'] = dic['totalNumber']
        data.append(movie)

    time = end - start

    # hive
    min = 0
    max = 0
    if 'min' in get_args.keys():
        min = get_args['min']
    if 'max' in get_args.keys():
        max = get_args['max']
    sql = 'select id,title,videoTime,points,totalNumber from movie where videoTime >= ' + \
        str(min) + ' and videoTime <= ' + str(max)
    begin = Time.time()
    cur.execute(sql)
    end = Time.time()
    hiveTime = end-begin
    results = cur.fetchall()

    # mysql

    data1 = []
    min = '0'
    max = '10000'
    url = ''
    if 'min' in get_args:
        min = get_args['min']
    if 'max' in get_args:
        max = get_args['max']
    time_start = Time.time()
    sql = 'select product_id,movie_title,VideoTime,Points,totalNumber from movies natural join product where '
    url = 'VideoTime>='+min+' and VideoTime<='+max
    sql = sql+url
    cursor.execute(sql)
    results = cursor.fetchall()
    time_end = Time.time()
    for row in results:
        movie = {}
        movie['id'] = row[0]
        movie['title'] = row[1]
        movie['videoTime'] = row[2]
        movie['points'] = row[3]
        movie['totalNumber'] = row[4]
        data1.append(movie)
    timecost = time_end-time_start
    return jsonify({
        'data': {
            'count': len(data1),
            'Neo4jTime': time*1000,
            'HiveTime': hiveTime*1000,
            'MySqlTime': timecost*1000,
            'movieDatas': data1
        }
    })

# 查询演员和导演的关系


@app.route('/api/connection', methods=['GET'])
def getMoviesByRelation():
    get_args = request.args.to_dict()
    data = []
    # Neo4j
    match_str = ""
    if 'Director' in get_args.keys():
        match_str = match_str + \
            "match (:Person)-[c:COOPERATE]->(:Person {name:'"+get_args['Director'] + \
            "'}) return c order by toInteger(c.cooperation_times) desc limit 10"
        start = Time.time()
        nodes = graph.run(match_str)
        end = Time.time()

        for node in nodes:
            rel = dict(node)
            startDir = dict(rel['c'].start_node)
            data.append(startDir['name'])

    else:
        match_str = match_str + \
            "match (:Person)-[c:COOPERATE]->(:Person) return c order by toInteger(c.cooperation_times) desc limit 10"
        start = Time.time()
        nodes = graph.run(match_str)
        for relation in nodes:
            pair = {}
            rel = dict(relation)
            # startId = rel['c'].start_node
            # endId = rel['c'].end_node
            # startNode = graph.run(f'match (p:Person) where ID(p)={startId} return p')
            # endNode = graph.run(f'match (p:Person) where ID(p)={endId} return p')
            startDir = dict(rel['c'].start_node)
            endDir = dict(rel['c'].end_node)
            pair['director'] = endDir['name']
            pair['actor'] = startDir['name']
            data.append(pair)

        end = Time.time()

    time = end - start

    # Hive
    if 'Director' in get_args.keys():
        director = get_args['Director']
        sql = 'select * from dir_act_relation where directorName = "' + \
            director + '" order by number desc limit 10'
    else:
        sql = 'select * from dir_act_relation order by number desc limit 10'
    begin = Time.time()
    cur.execute(sql)
    end = Time.time()
    hiveTime = end-begin
    results = cur.fetchall()

    # mysql
    time_start = Time.time()
    data2 = []
    url = ''
    if 'Director' in get_args:
        url = 'person_name="'+get_args['Director']+'"'
        sql = 'select actor_id,cooperation_times from cooperation_relation natural join persons where '
        sql = sql+url+' order by cooperation_times DESC'
        cursor.execute(sql)
        results = cursor.fetchall()
        counter = 0
        for row in results:
            counter = counter+1
            movie = {}
            movie['actor_id'] = row[0]
            movie['cooperation_times'] = row[1]
            data2.append(movie)
            if counter == 10:
                break
        data1 = []
        for row in data2:
            str1 = str(row['actor_id'])
            str2 = str(row['cooperation_times'])
            sql = 'select person_name from persons where person_id="'+str1+'"'
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                movie = {}
                movie['name'] = row[0]
                movie['times'] = str2
            data1.append(movie)
        time_end = Time.time()
        timecost = time_end-time_start
    else:
        sql = 'select person_id,actor_id,cooperation_times from cooperation_relation natural join persons'
        sql = sql+' order by cooperation_times DESC'
        cursor.execute(sql)
        results = cursor.fetchall()
        counter = 0
        for row in results:
            counter = counter+1
            movie = {}
            movie['director_id'] = row[0]
            movie['actor_id'] = row[1]
            movie['cooperation_times'] = row[2]
            data2.append(movie)
            if counter == 10:
                break
        data1 = []
        for row in data2:
            movie = {}
            str1 = str(row['director_id'])
            str2 = str(row['actor_id'])
            str3 = str(row['cooperation_times'])
            sql = 'select person_name from persons where person_id="'+str2+'"'
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                movie['actor'] = row[0]
            sql = 'select person_name from persons where person_id="'+str1+'"'
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                movie['director'] = row[0]
            movie['times'] = str3
            data1.append(movie)
        time_end = Time.time()
        timecost = time_end-time_start
    if 'Director' in get_args.keys():
        return jsonify({
            'data': {
                'Neo4jTime': time*1000,
                'HiveTime': hiveTime*1000,
                'MySqlTime': timecost*1000,
                'actors': data1
            }
        })
    else:
        return jsonify({
            'data': {
                'Neo4jTime': time*1000,
                'HiveTime': hiveTime*1000,
                'MySqlTime': timecost*1000,
                'pairs': data1
            }
        })


if __name__ == '__main__':
    server = pywsgi.WSGIServer(("0.0.0.0", int("{your_server_port}")), app)
    server.serve_forever()
    # app.run(host='0.0.0.0', port=12581, debug=True)
