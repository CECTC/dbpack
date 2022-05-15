<?php

$reqPath = strtok($_SERVER["REQUEST_URI"], '?');

$reaHeaders = getallheaders();
$xid = $reaHeaders['x_dbpack_xid'] ?? '';

if (empty($xid)) {
    die('xid is not provided!');
}

$aggregationSvc = new AggregationSvc();

switch ($reqPath) {
    case '/v1/order/create':
        if ($aggregationSvc->CreateSo($xid, false)) {
            http_response_code(200);
            echo json_encode([
                'success' => true,
                'message' => 'success',
            ]);
        } else {
            http_response_code(400);
            echo json_encode([
                'success' => false,
                'message' => 'fail',
            ]);
        }
        break;
    case '/v1/order/create2':
        if ($aggregationSvc->CreateSo($xid, true)) {
            http_response_code(200);
            echo json_encode([
                'success' => true,
                'message' => 'success',
            ]);
        } else {
            http_response_code(400);
            echo json_encode([
                'success' => false,
                'message' => 'fail',
            ]);
        }
        break;
    default:
        echo 'api not found';
}

