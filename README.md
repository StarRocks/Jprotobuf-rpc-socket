Jprotobuf-rpc-socket-starrocks
====================

This is a customized fork of [Baidu-ecom/Jprotobuf-rpc-socket](https://github.com/Baidu-ecom/Jprotobuf-rpc-socket), used internally by [StarRocks](https://github.com/StarRocks/starrocks).

> A Protobuf RPC implementation based on jprotobuf, adapted for StarRocks. It depends on [jprotobuf-starrocks](https://github.com/StarRocks/jprotobuf-starrocks) instead of `com.baidu:jprotobuf`.

Original repository: [https://github.com/Baidu-ecom/Jprotobuf-rpc-socket](https://github.com/Baidu-ecom/Jprotobuf-rpc-socket)

## Build status

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.starrocks/jprotobuf-rpc-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.starrocks/jprotobuf-rpc-core)


Protobuf RPC is a high-performance binary RPC protocol implementation over TCP. It uses Protobuf as its data exchange format and supports publishing services from plain POJOs, which greatly reduces development complexity.<br>
Features:<br>
- Publish services from plain POJOs, simple to use
- Built-in connection pool for high throughput and low latency (QPS: 50,000+)
- Automatic reconnection
- HA load balancing on the client side
- Attachment support
- Compression with GZip and Snappy
- Splitting and merging of large packets
- Multi-level timeout settings for flexible request timeout control
- Dynamic generation of RPC service metadata
- Built-in HTTP management (since 3.1.1)

Related projects:<br>
[https://github.com/StarRocks/jprotobuf-starrocks](https://github.com/StarRocks/jprotobuf-starrocks "https://github.com/StarRocks/jprotobuf-starrocks") (forked from [https://github.com/jhunters/jprotobuf](https://github.com/jhunters/jprotobuf "https://github.com/jhunters/jprotobuf"))<br>
Golang protocol implementation: [https://github.com/baidu-golang/baidurpc](https://github.com/baidu-golang/baidurpc "https://github.com/baidu-golang/baidurpc")


## Usage ##

Requirements: JDK 8+
```xml
<dependency>
	<groupId>com.starrocks</groupId>
	<artifactId>jprotobuf-rpc-core</artifactId>
	<version>1.0.0</version>
</dependency>

<!-- Spring extension -->
<dependency>
	<groupId>com.starrocks</groupId>
	<artifactId>jprotobuf-rpc-core-spring</artifactId>
	<version>1.0.0</version>
</dependency>

<!-- Service registry and discovery based on Redis -->
<dependency>
	<groupId>com.starrocks</groupId>
	<artifactId>jprotobuf-rpc-registry-redis</artifactId>
	<version>1.0.0</version>
</dependency>

```
Use the jprotobuf precompile plugin to precompile codecs and speed up startup:
```xml
    <plugin>
        <groupId>com.baidu</groupId>
        <artifactId>jprotobuf-precompile-plugin</artifactId>
        <version>1.2.8</version>
        <configuration>
            <skipErrorNoDescriptorsFound>true</skipErrorNoDescriptorsFound>
            <filterClassPackage>com.baidu</filterClassPackage>
        </configuration>
        <executions>
            <execution>
                <phase>compile</phase>
                <goals>
                    <goal>precompile</goal>
                </goals>
            </execution>
        </executions>
    </plugin>
```
`filterClassPackage` specifies the package to scan during precompilation. Only one package name is supported.<br>
Run it with Maven:<br>
```property
mvn jprotobuf:precompile
or
mvn package
```
[Download releases](https://repo1.maven.org/maven2/com/starrocks/jprotobuf-rpc-core/)
#### Quick Start ####
Jprotobuf-rpc-socket is built on top of JProtobuf, so you do not need to write Google Protobuf IDL files.

##### Client #####
1. Define the EchoService data object

EchoService provides an `echo` method whose parameter object, EchoInfo, has a single `message` field.
EchoInfo is defined as follows:
```java
public class EchoInfo {

    @Protobuf
    public String message;
}

```
The annotation-based definition saves a lot of work. It is equivalent to the following IDL:
```property
package pkg;

option java_package = "com.baidu.bjf.remoting.protobuf.rpc";

// the generated Java class name
option java_outer_classname = "EchoInfo";

message InterClassName {
  required string message = 1;
}


```
2. Define the EchoService interface
```java
public interface EchoService {

    /**
     * To define a RPC client method. <br>
     * serviceName is "echoService"
     * methodName is use default method name "echo"
     * onceTalkTimeout is 200 milliseconds
     *
     * @param info
     * @return
     */
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 200)
    EchoInfo echo(EchoInfo info);
}

```
RPC methods must be annotated with `@ProtobufRPC`. `serviceName` and `methodName` must match the server side.
Since `methodName` is not specified here, the method name `echo` is used.


3. Create an RPC client and call the service
```java
RpcClient rpcClient = new RpcClient();
// create the EchoService proxy
ProtobufRpcProxy<EchoService> pbrpcProxy = new ProtobufRpcProxy<EchoService>(rpcClient, EchoService.class);
pbrpcProxy.setPort(1031);
// generate the proxy instance
EchoService echoService = pbrpcProxy.proxy();
EchoInfo request = new EchoInfo();
request.message = "hello";
EchoInfo response = echoService.echo(request);
rpcClient.stop();
```

##### Server #####
1. Implement the service
```java
public class EchoServiceImpl {

    @ProtobufRPCService(serviceName = "echoService", methodName = "echo")
    public EchoInfo doEcho(EchoInfo info) {
        EchoInfo ret = new EchoInfo();
        ret.setMessage("hello:" + info.message);

        return ret;
    }
}
```
Published RPC methods must be annotated with `@ProtobufRPCService`.

2. Publish the RPC service
```java
	RpcServer rpcServer = new RpcServer();

	EchoServiceImpl echoServiceImpl = new EchoServiceImpl();
	rpcServer.registerService(echoServiceImpl);
	rpcServer.start(1031);
```
The code above publishes the RPC service of EchoServiceImpl.

[More usage](user_guide.md), original project wiki: [User-Guide](https://github.com/Baidu-ecom/Jprotobuf-rpc-socket/wiki/User-Guide)

## Performance ##

Machine:
- Linux, 64 GB memory, 6 cores / 12 threads
- Intel(R) Xeon(R) CPU           E5645  @ 2.40GHz

Results (client and server on the same machine):
Single thread: average QPS 9,000+
Multiple threads: peak QPS 40,000+
```property
---------------------Performance Result-------------------------
send byte size: 44;receive byte size: 50
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|               11807|                     0|                8469|                   1|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 44;receive byte size: 50
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|               10407|                     0|                9608|                   1|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 1139;receive byte size: 1139
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|               11513|                     0|                8685|                   1|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 44;receive byte size: 50
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|                5904|                     0|               16937|                   2|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 44;receive byte size: 50
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|                3754|                     0|               26638|                   4|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 44;receive byte size: 50
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|                1736|                     0|               57603|                  20|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 1139;receive byte size: 1139
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|                2381|                     0|               41999|                  20|
---------------------Performance Result-------------------------
---------------------Performance Result-------------------------
send byte size: 1139;receive byte size: 1139
|         total count|       time took(ms)|           average(ms)|                 QPS|             threads|
|              100000|                2012|                     0|               49701|                  40|
---------------------Performance Result-------------------------
```

## License ##

Licensed under the [Apache License, Version 2.0](LICENSE).
