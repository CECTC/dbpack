<?php

require './svc/svc.php';

$reqPath = strtok($_SERVER["REQUEST_URI"], '?');
$reaHeaders = getallheaders();

// "x" is in upper case here returned by getallheaders() function
$xid = $reaHeaders['X_dbpack_xid'] ?? '';

if (empty($xid)) {
    die('xid is not provided!');
}

$aggregationSvc = new AggregationSvc();

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    switch ($reqPath) {
        case '/v1/order/create':
            if ($aggregationSvc->CreateSo($xid, false)) {
                responseOK();
            } else {
                responseError();
            }
            break;
        case '/v1/order/create2':
            if ($aggregationSvc->CreateSo($xid, true)) {
                responseOK();
            } else {
                responseError();
            }
            break;
        default:
            die('api not found');
    }
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