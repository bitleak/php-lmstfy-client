<?php

namespace Lmstfy;

class Client {
    private $addr; 
    private $namespace;
    private $token;
    private $url;


    /**
     * constructor
     *
     * @var String $addr        server address, eg: "127.0.0.1:9999"
     * @var String $namespace   namespace of queue 
     * @var String $token       secret to access the namespace
     */
    public function __construct($addr, $namespace, $token) {
        $this->addr = $addr;
        $this->namespace = $namespace;
        $this->token = $token;
        $this->url = join('/', array(trim($this->addr, '/'), 'api', $this->namespace));
        $this->ch = curl_init();
    }

    /**
     * Publish a job to task queue
     *
     * @var String $queue       publish to which queue
     * @var String $data        job data to store
     * @var Int    $ttl         time to live(unit: Second). TTL should be always gt the delay, or the task would never be consumed
     * @var Int    $tries       max retry times of the job. If tires = 1, the job would be consumed at most once
     * @var Int    $delay       job could be consumed after delay seconds
     */
    public function Publish($queue, $data, $ttl = 0, $tries = 1, $delay = 0) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        if ($ttl < 0) {
            throw new \Exception("ttl(time to live) should be >= 0");
        }
        if ($tries <= 0) {
            throw new \Exception("retries should be > 0");
        }
        if ($delay < 0) {
            throw new \Exception("delay should be >= 0");
        }

        $query = array('ttl'=>$ttl, 'tries'=>$tries, 'delay'=>$delay);
        $response = $this->doRequest($queue, 'PUT', http_build_query($query), $data);
        if ($response['code'] != 201) {
            throw new \Exception("failed to publish while got bad response code:".$response['code']);
        }
        $body = json_decode($response['body'], true);
        return $body['job_id'];
    }

    /*
     * Consume job from task queue
     * @var String $queue       queue name
     * @var Int    $ttr         time to run(unit: Second). If consumer didn't ack after exceed the ttr,
     *                          the job would be re-consumed if retry times wasn't reached, or fall into deadletter queue.
     *                          The job in deadletter queue would be disappeared til it's respwan by the user manually.
     * @var Int    $timeout     client blocking wait for new job(unit: Second), 0 would be blocking forever
     */
    public function Consume($queue, $ttr = 120, $timeout = 0) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        if ($ttr <= 0) {
            throw new \Exception("ttr(time to run) should be > 0");
        }
        if ($timeout < 0 || $timeout > 600) {
            throw new \Exception("timeout should be >= 0 && <= 600");
        }

        $query = array('ttr'=>$ttr, 'timeout'=>$timeout);
        $response = $this->doRequest($queue, 'GET', http_build_query($query));
        if ($response['code'] == 404) {
            return NULL;
        }
        if ($response['code'] != 200) {
            throw new \Exception("failed to consume while got bad response code:".$response['code']);
        }
        $job = json_decode($response['body'], true);
        $job['data'] = base64_decode($job['data']);
        return $job;
    }

    /*
     * Ack job, and server would delete the job 
     * @var String $queue   queue name
     * @var String $job_id  job id 
     */
    public function Ack($queue, $job_id) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        if (empty($job_id)) {
            throw new \Exception("job id can't be empty");
        }
        $response = $this->doRequest($queue.'/job/'.$job_id, 'DELETE');
        if ($response['code'] != 204) {
            throw new \Exception("failed to ack while got bad response code:".$response['code']);
        }
        return true;
    }

    /*
     * Get job from queue
     * @var String $queue   queue name
     * @var String $job_id  job id
     */
    public function GetJob($queue, $job_id) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        if (empty($job_id)) {
            throw new \Exception("job id can't be empty");
        }
        $response = $this->doRequest($queue.'/job/'.$job_id, 'GET');
        if ($response['code'] != 200) {
            throw new \Exception("failed to consume while got bad response code:".$response['code']);
        }
        $job = json_decode($response['body'], true);
        $job['data'] = base64_decode($job['data']);
        return $job;
    }

    /*
     * Queue return the size of queue
     * @var String $queue   queue name
     */
    public function QueueSize($queue) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        $response = $this->doRequest($queue.'/size', 'GET');
        if ($response['code'] != 200) {
            throw new \Exception("failed to get queue size while got bad response code:".$response['code']);
        }
        $body = json_decode($response['body'], true);
        return $body['size'];
    }

    /*
     * Queue return the size of queue
     * @var String $queue   queue name
     */
    public function PeekQueue($queue) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        $response = $this->doRequest($queue.'/peek', 'GET');
        if ($response['code'] != 200) {
            throw new \Exception("failed to peek queue while got bad response code:".$response['code']);
        }
        $job = json_decode($response['body'], true);
        $job['data'] = base64_decode($job['data']);
        return $job;
    }
    
    /*
     * PeekDeadLetter peek a job from dead letter
     * @var String $queue   queue name
     */
    public function PeekDeadLetter($queue) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        $response = $this->doRequest($queue.'/deadletter', 'GET');
        if ($response['code'] != 200) {
            throw new \Exception("failed to peek dead letter while got bad response code:".$response['code']);
        }
        $job = json_decode($response['body'], true);
        $job['data'] = base64_decode($job['data']);
        return $job;
    }

    /*
     * RespawnDeadLetter respawn job from dead letter 
     * @var String $queue   queue name
     * @var Int    $limit   max number of job to respawn 
     * @var Int    $ttl     time to live
     */
    public function RespawnDeadLetter($queue, $limit, $ttl) {
        if (empty($queue)) {
            throw new \Exception("queue name can't be empty");
        }
        if ($limit < 0) {
            throw new \Exception("limit should be > 0");
        }
        if ($ttl < 0) {
            throw new \Exception("ttl should be >= 0");
        }
        $query = array('limit'=>$limit, 'ttl'=>$ttl);
        $response = $this->doRequest($queue.'/deadletter', 'PUT');
        if ($response['code'] != 200) {
            throw new \Exception("failed to respawn dead letter while got bad response code:".$response['code']);
        }
        $body = json_decode($response['body'], true);
        return $body['count'];
    }

    /*
     * ConsumeMultiQueues can be used to impl the prio queue,
     * server would fetch job from Q1, Q2, ... QN with order
     * @var Int    $ttl     time to live
     * @var Int    $timeout client blocking wait for new job(unit: Second)
     */
    public function ConsumeMultiQueues($ttr, $timeout, ...$queues) {
        if ($ttr <= 0) {
            throw new \Exception("ttr(time to run) should be > 0");
        }
        if ($timeout < 0 || $timeout > 600) {
            throw new \Exception("timeout should be >= 0 && <= 600");
        }
        if (count($queues) <= 0) {
            throw new \Exception("consume atleast one queue");
        }
        $query = array('ttr'=>$ttr, 'timeout'=>$timeout);
        $response = $this->doRequest(join(',', $queues), 'GET', $query);
        if ($response['code'] != 200) {
            throw new \Exception("failed to consume while got bad response code:".$response['code']);
        }
        $job = json_decode($response['body'], true);
        $job['data'] = base64_decode($job['data']);
        return $job;
    }

    public function Close() {
        curl_close($this->ch);
        $this->ch = NULL;
    }

    private function doRequest($relativePath, $method, $query='', $data='') {
        if ($this->ch == NULL) {
            $this->ch = curl_init();
        }
        $headers= array(
            "X-Token:".$this->token
        );
        $url = $this->url.'/'.$relativePath;
        if (!empty($query)) {
            $url = $url.'?'.$query;
        }
        $ch = $this->ch;
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        // connect timeout 1500ms
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT_MS, 1500);
        // socket timeout 300s
        curl_setopt($ch, CURLOPT_TIMEOUT_MS, 300000);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        if (!empty($data)) {
            if (is_string($data)) {
                curl_setopt($ch, CURLOPT_POSTFIELDS,$data);
            } else {
                curl_setopt($ch, CURLOPT_POSTFIELDS,http_build_query($data));
            }
        }
        $body = curl_exec($ch);
        $errno = curl_errno($ch);
        if ($errno != 0 && $errno != CURLE_HTTP_NOT_FOUND) {
            $this->Close();
            throw new \Exception("failed to curl while error:".curl_strerror($errno));
        }
        $res = array(
            'code' => curl_getinfo($ch, CURLINFO_HTTP_CODE),
            'body' => $body
        );
        return $res;
    }
}
