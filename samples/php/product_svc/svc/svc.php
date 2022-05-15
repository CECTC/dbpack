<?php



class ProductDB
{
    private PDO $_connection;
    private static ProductDB $_instance;
    private string $_host = '127.0.0.1';
    private int $_port = 13307;
    private string $_username = 'root';
    private string $_password = '';
    private string $_database = 'product';

    const allocateInventorySql = "update /*+ XID('%s') */ product.inventory set available_qty = available_qty - ?, 
		allocated_qty = allocated_qty + ? where product_sysno = ? and available_qty >= ?";

    public static function getInstance(): ProductDB
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

    public function allocateInventory(string $xid, array $inventories): bool
    {
        $this->getConnection()->beginTransaction();

        foreach ($inventories as $inventory) {
            $allocateInventorySql = sprintf(self::allocateInventorySql, $xid);

            $result = $this->getConnection()->prepare($allocateInventorySql)->execute([
                $inventory['Qty'],
                $inventory['Qty'],
                $inventory['ProductSysNo'],
                $inventory['Qty'],
            ]);
            if (!$result) {
                $this->getConnection()->rollBack();
            }
        }
        return $this->getConnection()->commit();
    }
}

