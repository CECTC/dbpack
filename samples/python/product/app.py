from flask import Flask, jsonify, request
import mysql.connector


app = Flask(__name__)

conn = mysql.connector.connect(
  host="127.0.0.1",
  port="13307",
  user="dksl",
  password="123456",
  database="product",
)
 
cursor = conn.cursor(prepared=True)

allocate_inventory_sql = "update /*+ XID(%s) */ product.inventory set available_qty = available_qty - %s, allocated_qty = allocated_qty + %s where product_sysno = %s and available_qty >= %s;"

@app.route('/allocateInventory', methods=['POST'])
def create_so():
    xid = request.headers.get('xid')
    reqs = request.get_json()
    if xid and "req" in reqs:
        for res in reqs["req"]:
            try:
                cursor.execute(allocate_inventory_sql, (xid, res["qty"], res["qty"], res["product_sysno"], res["qty"]))
            except Exception as e:
                print(e.args)
        
        return jsonify(dict(success=True, message="success")), 200
        
    return jsonify(dict(success=False, message="failed")), 400

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3002)