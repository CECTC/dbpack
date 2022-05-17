from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/v1/order/create', methods=['POST'])
def create_1():
   return create_so(rollback=False)
    
@app.route('/v1/order/create2', methods=['POST'])
def create_2():
   return create_so(rollback=True)

def create_so(rollback=True):
    xid = request.headers.get("x_dbpack_xid")

    so_items = [dict(
        product_sysno=1,
        product_name="apple iphone 13",
        original_price=6799,
        cost_price=6799,
        deal_price=6799,
        quantity=2,
    )]

    so_master = [dict(
        buyer_user_sysno = 10001,
        seller_company_code = "SC001",
        receive_division_sysno = 110105,
        receive_address = "beijing",
        receive_zip = "000001",
        receive_contact = "scott",
        receive_contact_phone =  "18728828296",
        stock_sysno = 1,
        payment_type = 1,
        so_amt = 6999 * 2,
        status = 10,
        appid = "dk-order",
        so_items = so_items,
    )]

    success = (jsonify(dict(success=True, message="success")), 200)
    failed = (jsonify(dict(success=False, message="failed")), 400)
    headers = {
        "Content-Type": "application/json",
        "xid": xid
    }

    so_req = dict(req=so_master)
    resp1 = requests.post("http://localhost:3001/createSo", headers=headers, json=so_req, timeout=30)
    if resp1.status_code == 400:
        return failed

    ivt_req = dict(req=[dict(product_sysno= 1, qty=2)])
    resp2 = requests.post("http://localhost:3002/allocateInventory", headers=headers, json=ivt_req, timeout=30)
    if resp2.status_code == 400:
        return failed

    if rollback:
        print("rollback")
        return failed

    return success

if __name__ == "__main__":
    app.run(port=3000)
