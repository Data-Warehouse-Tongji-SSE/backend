from flask import Flask,jsonify
from flask.json import tojson_filter
import pymysql

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
def mysqlConnectionTest(id):
    print(id)
    sql = 'select movie_id,movie_title from movies'
    cursor.execute(sql)
    results = cursor.fetchall()
    names = []
    title="1321"
    for row in results:
        movie_id = row[0]
        if movie_id == id:
            title=row[1]
            break  
    print(title)
    return title
    

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=5000,debug=True)
