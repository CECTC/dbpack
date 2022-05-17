<?php



class OrderDB
{
    private PDO $_connection;
    private static OrderDB $_instance;
    private string $_host = '127.0.0.1';
    private int $_port = 13308;
    private string $_username = 'hehe';
    private string $_password = 'hehe';
    private string $_database = 'order';

    const insertSoMaster = "INSERT /*+ XID('%s') */ INTO order.so_master (sysno, so_id, buyer_user_sysno, seller_company_code, 
		receive_division_sysno, receive_address, receive_zip, receive_contact, receive_contact_phone, stock_sysno, 
        payment_type, so_amt, status, order_date, appid, memo) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?)";

    const insertSoItem = "INSERT /*+ XID('%s') */ INTO order.so_item(sysno, so_sysno, product_sysno, product_name, cost_price, 
		original_price, deal_price, quantity) VALUES (?,?,?,?,?,?,?,?)";

    public static function getInstance(): OrderDB
    {
        if (empty(self::$_instance)) {
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
                [
                    PDO::ATTR_PERSISTENT => true,
                    PDO::ATTR_EMULATE_PREPARES => false,
                ]
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
            if (!$this->insertSo($xid, $master)) {
                $this->getConnection()->rollBack();
            }
        }
        return $this->getConnection()->commit();
    }

    private function insertSo(string $xid, array $soMaster): bool
    {
        $soId = hrtime(true);
        $memo = '';
        $insertSoMasterSql = sprintf(self::insertSoMaster, $xid);

        $statement = $this->getConnection()->prepare($insertSoMasterSql);
        $statement->bindValue(1, $soId);
        $statement->bindValue(2, $soId);
        $statement->bindValue(3, $soMaster['buyerUserSysNo']);
        $statement->bindValue(4, $soMaster['sellerCompanyCode']);
        $statement->bindValue(5, $soMaster['receiveDivisionSysNo']);
        $statement->bindValue(6, $soMaster['receiveAddress']);
        $statement->bindValue(7, $soMaster['receiveZip']);
        $statement->bindValue(8, $soMaster['receiveContact']);
        $statement->bindValue(9, $soMaster['receiveContactPhone']);
        $statement->bindValue(10, $soMaster['stockSysNo']);
        $statement->bindValue(11, $soMaster['paymentType']);
        $statement->bindValue(12, $soMaster['soAmt']);
        $statement->bindValue(13, $soMaster['status']);
        $statement->bindValue(14, $soMaster['appID']);
        $statement->bindValue(15, $memo);

        $result = $statement->execute();
        if (!$result) {
            return false;
        }
        $insertSoItemSql = sprintf(self::insertSoItem, $xid);
        foreach ($soMaster['soItems'] as $item) {
            $soItemId = hrtime(true);
            $statement = $this->getConnection()->prepare($insertSoItemSql);
            $statement->bindValue(1, $soItemId);
            $statement->bindValue(2, $soId);
            $statement->bindValue(3, $item['productSysNo']);
            $statement->bindValue(4, $item['productName']);
            $statement->bindValue(5, $item['costPrice']);
            $statement->bindValue(6, $item['originalPrice']);
            $statement->bindValue(7, $item['dealPrice']);
            $statement->bindValue(8, $item['quantity']);

            $result = $statement->execute();
            if (!$result) {
                return false;
            }
        }
        return true;
    }
}

