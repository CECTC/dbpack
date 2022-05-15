<?php

class AggregationSvc
{

    public function CreateSo(string $xid, bool $rollback): bool
    {
        $createSoSuccess = $this->createSoRequest($xid);
        if (!$createSoSuccess) {
            return false;
        }
        $allocateInventorySuccess = $this->allocateInventoryRequest($xid);
        if (!$allocateInventorySuccess) {
            return false;
        }
        if ($rollback) {
            return false;
        }
        return true;
    }

    protected function createSoRequest(string $xid): bool
    {
        $soMasters = [
            [
                'buyerUserSysNo' => 10001,
                'sellerCompanyCode' => 'SC001',
                'receiveDivisionSysNo' => 110105,
                'receiveAddress' => 'beijing',
                'receiveZip' => '000001',
                'receiveContact' => 'scott',
                'receiveContactPhone' => '18728828296',
                'stockSysNo' => 1,
                'paymentType' => 1,
                'soAmt' => 6999 * 2,
                'status' => 10,
                'appID' => 'dk-order',
                'soItems' => [
                    [
                        'productSysNo' => 1,
                        'productName' => "apple iphone 13",
                        'costPrice' => 6799,
                        'originalPrice' => 6799,
                        'dealPrice' => 6999,
                        'quantity' => 2,
                    ],
                ],
            ],
        ];
        $url = 'http://localhost:3001/createSo';
        $response = $this->sendRequest($url, $xid, $soMasters);
        if ($response === false) {
            return false;
        }
        $responseData = json_decode($response, true);
        return $responseData['success'] ?? false;
    }

    protected function allocateInventoryRequest(string $xid): bool
    {
        $allocateInventoryReq = [
            [
                'ProductSysNo' => 1,
                'Qty' => 2,
            ],
        ];
        $url = 'http://localhost:3002/allocateInventory';
        $response = $this->sendRequest($url, $xid, $allocateInventoryReq);
        if ($response === false) {
            return false;
        }
        $responseData = json_decode($response, true);
        return $responseData['success'] ?? false;
    }

    private function sendRequest(string $url, string $xid, array $data): bool|string
    {
        $ch = curl_init($url);
        curl_setopt_array($ch, [
            CURLOPT_POST => true,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => array(
                'Content-Type: application/json',
                'xid: ' . $xid,
            ),
            CURLOPT_POSTFIELDS => json_encode($data)
        ]);

        $response = curl_exec($ch);
        if ($response === false) {
            return false;
        }
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        if ($httpCode == 400) {
            return false;
        }
        curl_close($ch);

        return $response;
    }
}