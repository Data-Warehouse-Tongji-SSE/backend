from flask import Flask,jsonify
import pymysql

db = pymysql.connect('localhost','root','123','moviedb')# 记得修改数据库地址
cursor = db.cursor()

app = Flask(__name__)

@app.route('/<id>',methods=['GET'])
def mysqlConnectionTest(id):
    sql = 'select 名字 from 影片'
    cursor.execute(sql)
    results = cursor.fetchall()
    names = []
    for row in results:
        name = row[0]
        names.append(name)
        if row == id:break
    return names
    

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=5000,debug=True)