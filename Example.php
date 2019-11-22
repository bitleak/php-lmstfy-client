<?php

require 'vendor/autoload.php';

use Lmstfy\Client;

$addr = "127.0.0.1:7777";
$namespace = "guest";
$token = "01CG17XY4HA7CQCMHSS8GTQ";
$queue = "test-queue";

$cli = new Client($addr, $namespace, $token);
// publish new job
$cli->Publish($queue, "bar", 0, 10, 0);
// consume the job from queue
$job = $cli->Consume($queue, 5, 3);
// process job 
var_dump($job);
// ack job
$cli->Ack($queue, $job["job_id"]);
$cli->Close();
