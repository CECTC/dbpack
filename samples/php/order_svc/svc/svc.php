<?php



class OrderDB
{
    private PDO $_connection;
    private static OrderDB $_instance;
    private string $_host = '127.0.0.1';
    private int $_port = 13308;
    private string $_username = 'root';
    private string $_password = '';
    private string $_database = 'order';

    const insertSoMaster = "INSERT /*+ XID('%s') */ INTO order.so_master (sysno, so_id, buyer_user_sysno, seller_company_code, 
		receive_division_sysno, receive_address, receive_zip, receive_contact, receive_contact_phone, stock_sysno, 
        payment_type, so_amt, status, order_date, appid, memo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?)";

    const insertSoItem = "INSERT /*+ XID('%s') */ INTO order.so_item(sysno, so_sysno, product_sysno, product_name, cost_price, 
		original_price, deal_price, quantity) VALUES (?,?,?,?,?,?,?,?)";

    public static function getInstance(): OrderDB
    {
        if (!self::$_instance) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    private function __construct()
    {
        try {
            $this->_connection = new PDO(
                "mysql:host=$this->_host;port=$this->_port;dbname=$this->_database;charset=utf8",
                $this->_username,
                $this->_password,
            );
        } catch (PDOException $e) {
            die($e->getMessage());
        }
    }

    private function __clone()
    {
    }

    public function getConnection(): PDO
    {
        return $this->_connection;
    }

    public function createSo(string $xid, array $soMasters): bool
    {
        $this->getConnection()->beginTransaction();

        foreach ($soMasters as $master) {
            if (!$this->insertSoMaster($xid, $master)) {
                $this->getConnection()->rollBack();
            }
        }
        return $this->getConnection()->commit();
    }

    private function insertSoMaster(string $xid, array $soMaster): bool
    {
        $soId = hrtime(true);
        $insertSoMasterSql = sprintf(self::insertSoMaster, $xid);

        $result = $this->getConnection()->prepare($insertSoMasterSql)->execute([
            $soId,
            $soId,
            $soMaster['buyerUserSysNo'],
            $soMaster['sellerCompanyCode'],
            $soMaster['receiveDivisionSysNo'],
            $soMaster['receiveAddress'],
            $soMaster['receiveZip'],
            $soMaster['receiveContact'],
            $soMaster['receiveContactPhone'],
            $soMaster['stockSysNo'],
            $soMaster['paymentType'],
            $soMaster['soAmt'],
            $soMaster['status'],
            $soMaster['appID'],
            '',
        ]);
        if (!$result) {
            return false;
        }
        $insertSoItemSql = sprintf(self::insertSoItem, $xid);
        foreach ($soMaster['soItems'] as $item) {
            $soItemId = hrtime(true);

            $result = $this->getConnection()->prepare($insertSoItemSql)->execute([
                $soItemId,
                $soId,
                $item['productSysNo'],
                $item['productName'],
                $item['costPrice'],
                $item['originalPrice'],
                $item['dealPrice'],
                $item['quantity'],
            ]);
            if (!$result) {
                return false;
            }
        }
        return true;
    }
}

