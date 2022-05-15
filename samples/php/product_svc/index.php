<?php

$reqPath = strtok($_SERVER["REQUEST_URI"], '?');

$reaHeaders = getallheaders();
$xid = $reaHeaders['x_dbpack_xid'] ?? '';

if (empty($xid)) {
    die('xid is not provided!');
}

if ($reqPath === 'allocateInventory') {
    $reqBody = file_get_contents('php://input');
    $inventories = json_decode($reqBody, true);

    $productDB = ProductDB::getInstance();
    $result = $productDB->allocateInventory($xid, $inventories);

    if ($result) {
        responseOK();
    } else {
        responseError();
    }
} else {
    echo 'api not exist';
}

function responseOK() {
    http_response_code(200);
    echo json_encode([
        'success' => true,
        'message' => 'success',
    ]);
}

function responseError() {
    http_response_code(400);
    echo json_encode([
        'success' => false,
        'message' => 'fail',
    ]);
}