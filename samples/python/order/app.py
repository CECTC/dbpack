from flask import Flask, jsonify, request
from datetime import datetime

import random

import mysql.connector


app = Flask(__name__)

conn = mysql.connector.connect(
  host="127.0.0.1",
  port="13308",
  user="dksl",
  password="123456",
  database="order",
)
 
cursor = conn.cursor(prepared=True)

insert_so_master = "INSERT /*+ XID('{xid}') */ INTO order.so_master({keys}) VALUES ({placeholders})"
insert_so_item = "INSERT /*+ XID('{xid}') */ INTO order.so_item({keys}) VALUES ({placeholders})"

@app.route('/createSo', methods=['POST'])
def create_so():
    xid = request.headers.get('xid')
    reqs = request.get_json()
    if xid and "req" in reqs:
        for res in reqs["req"]:
            res["sysno"] = next_id()
            res["so_id"] = res["sysno"]
            res["order_date"] = datetime.now()
            res_keys = [str(k) for k,v in res.items() if k != "so_items" and str(v) != ""]
            so_master = insert_so_master.format(
                xid=xid,
                keys=", ".join(res_keys),
                placeholders=", ".join(["%s"] * len(res_keys)),
            )

            try:
                cursor.execute(so_master, tuple(res.get(k, "") for k in res_keys))
            except Exception as e:
                print(e.args)
             
            so_items = res["so_items"]
            for item in so_items:
                item["sysno"] = next_id()
                item["so_sysno"] = res["sysno"]
                item_keys = [str(k) for k,v in item.items() if str(v) != "" ]
                so_item = insert_so_item.format(
                    xid=xid,
                    keys=", ".join(item_keys),
                    placeholders=", ".join(["%s"] * len(item_keys)),
                )
                try:
                    cursor.execute(so_item, tuple(item.get(k, "") for k in item_keys))
                except Exception as e:
                    print(e.args)
 
        return jsonify(dict(success=True, message="success")), 200
    
    return jsonify(dict(success=False, message="failed")), 400 

def next_id():
    return random.randrange(0, 9223372036854775807)
  

if __name__ == '__main__':
   app.run(host="0.0.0.0", port=3001)
