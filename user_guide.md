#### Quick Start ####
##### Client #####
1. Define the EchoService data object

EchoService provides an `echo` method whose parameter object, EchoInfo, has a single `message` field.
EchoInfo is defined as follows:
```java
public class EchoInfo {
    
    @Protobuf(order = 1)
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

#### Attachments ####
Protobuf is not well suited for serializing large payloads, so large data can be sent as an attachment:

##### Client #####
```java
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 450, 
            attachmentHandler = EchoClientAttachmentHandler.class, logIDGenerator = EchoLogIDGenerator.class)
    EchoInfo echoWithAttachement(EchoInfo info);
```
An example EchoClientAttachmentHandler implementation:
```java
public class EchoClientAttachmentHandler implements ClientAttachmentHandler {

    private byte[] attachment = EchoClientAttachmentHandler.class.getName().getBytes();

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.baidu.jprotobuf.pbrpc.AttachmentHandler#handleRequest(java.lang.String
     * , java.lang.String, java.lang.Object[])
     */
    public byte[] handleRequest(String serviceName, String methodName, Object... params) {
        return attachment;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.baidu.jprotobuf.pbrpc.ClientAttachmentHandler#handleResponse(byte[],
     * java.lang.String, java.lang.String, java.lang.Object[])
     */
    public void handleResponse(byte[] response, String serviceName, String methodName, Object... params) {
        Assert.assertEquals(EchoServerAttachmentHandler.class.getName(), new String(response));

    }

}
```
##### Server #####
```java
    @ProtobufRPCService(serviceName = "echoService", methodName = "echoWithAttachement", 
            attachmentHandler = EchoServerAttachmentHandler.class)
    public EchoInfo dealWithAttachement(EchoInfo info) {
        return doEcho(info);
    }
```
An example EchoServerAttachmentHandler implementation:
```java
public class EchoServerAttachmentHandler implements ServerAttachmentHandler {

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.baidu.jprotobuf.pbrpc.ServerAttachmentHandler#handleAttachement(byte
     * [], java.lang.String, java.lang.String, java.lang.Object[])
     */
    public byte[] handleAttachement(byte[] response, String serviceName, String methodName, Object... params) {
        Assert.assertEquals(EchoClientAttachmentHandler.class.getName(), new String(response));
        return EchoServerAttachmentHandler.class.getName().getBytes();
    }

}
```

#### Compression ####

Enabling compression is simple: just specify the compression type on the client side. GZIP and SNAPPY are supported.

```java
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 1500, compressType = CompressType.GZIP,
            attachmentHandler = EchoClientAttachmentHandler.class, logIDGenerator = EchoLogIDGenerator.class)
    EchoInfo echoGzip(EchoInfo info);
    
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 1500, compressType = CompressType.Snappy,
            attachmentHandler = EchoClientAttachmentHandler.class, logIDGenerator = EchoLogIDGenerator.class)
    EchoInfo echoSnappy(EchoInfo info);
```

#### Load Balancing ####
Supported since 2.16, provided by HaProtobufRpcProxy.

HaProtobufRpcProxy gets its server list from a NamingService implementation:
```java
public interface NamingService {

    /**
     * get server list from naming service.
     * @return server list.
     * @throws Exception in case of any exception
     */
    List<InetSocketAddress> list() throws Exception;
  
}
```

Here is an example.
Creating a HaProtobufRpcProxy is simple:

```java
HaProtobufRpcProxy<EchoService> pbrpcProxy =
                    new HaProtobufRpcProxy<EchoService>(rpcClient, EchoService.class, getNamingService());
EchoService proxy = pbrpcProxy.proxy();
```
Important:
The code above does not specify a load balancing strategy, so round robin is used and every server has weight 1. If a server fails during load balancing, it is removed from the list and a background heartbeat check is started (once per second by default). The heartbeat only uses the ping interface.


For testing, here is a mock NamingService implementation:
```java
public class DummyNamingService implements NamingService {
    
    private List<InetSocketAddress> list;
    
    /**
     * @param list
     */
    public DummyNamingService(List<InetSocketAddress> list) {
        super();
        this.list = list;
    }

    /* (non-Javadoc)
     * @see com.baidu.jprotobuf.pbrpc.client.ha.NamingService#list()
     */
    public List<InetSocketAddress> list() throws Exception {
        return list;
    }

}
		// usage
        address = new InetSocketAddress(1035);
        list.add(address);

        namingService = new DummyNamingService(list);

```

For more examples, see the unit test com.baidu.jprotobuf.pbrpc.client.ha.HaEchoServiceTest.


#### Spring Integration ####
Supported since 2.17, with both XML configuration and annotations.
##### XML configuration #####
**Export an RPC server service with RpcServiceExporter<br>**
RpcServiceExporter exposes the EchoServiceImpl object as an RPC service. Clients can then access it through RpcProxyFactoryBean or the API.

First, annotate the methods to publish with @ProtobufRPCService:

```java
public class EchoServiceImpl {
    @ProtobufRPCService(serviceName = "echoService", methodName = "echo")
    public EchoInfo doEcho(EchoInfo info) {
        EchoInfo ret = new EchoInfo();
        ret.setMessage("hello:" + info.getMessage());
        return ret;
    }
```

Then publish the service with RpcServiceExporter:
```xml
	<bean id="echoService" class="com.baidu.jprotobuf.pbrpc.EchoServiceImpl"></bean>

	<bean class="com.baidu.jprotobuf.pbrpc.spring.RpcServiceExporter">
		<property name="servicePort" value="1031"></property>
		<property name="registerServices">
			<list>
				<ref local="echoService" />
			</list>
		</property>
        <property name="connectTimeout" value="1000"></property>
	</bean>

```
The XML above publishes the doEcho method of EchoServiceImpl as an RPC service. Service name: echoService, method: echo, port: 1031.

Note: RpcServiceExporter extends RpcServiceOptions, so all additional RPC options can be set as properties.

**Connect a client with RpcProxyFactoryBean<br>**
For the service published above, define an interface:
```java
public interface EchoService {
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 1000)
    EchoInfo echo(EchoInfo info);
}
```

To connect a client to the service, create a separate Spring container that contains the interface and the connection settings:
```xml
	<bean id="echoServiceProxy" class="com.baidu.jprotobuf.pbrpc.spring.RpcProxyFactoryBean">
		<property name="serviceInterface" value="com.baidu.jprotobuf.pbrpc.EchoService"></property>
		<property name="port" value="1031"></property>
	</bean>
```

Spring then creates a proxy bean named echoServiceProxy of type EchoService.

Note: RpcProxyFactoryBean extends RpcClientOptions, so all additional RPC options can be set as properties.

**Connect a client with HaRpcProxyFactoryBean<br>**
HaRpcProxyFactoryBean is the Spring integration for load balancing. The configuration is simple:

```xml
	<bean id="namingService" class="com.baidu.jprotobuf.pbrpc.spring.UrlBasedNamingService">
		<constructor-arg>
			<value>localhost:1031;localhost:1032;localhost:1033</value>
		</constructor-arg>
	</bean>


	<bean id="echoServiceProxy" class="com.baidu.jprotobuf.pbrpc.spring.HaRpcProxyFactoryBean">
		<property name="serviceInterface" value="com.baidu.jprotobuf.pbrpc.EchoService"></property>
		<property name="namingService" ref="namingService"></property>
	</bean>
```


##### Annotation configuration #####
Annotation configuration is much simpler than XML and is the recommended way.

Whether publishing services or declaring clients, the following only needs to be configured once:

```xml
	<bean
		class="com.baidu.jprotobuf.pbrpc.spring.annotation.CommonAnnotationBeanPostProcessor">
		<property name="callback">
			<bean
				class="com.baidu.jprotobuf.pbrpc.spring.annotation.ProtobufRpcAnnotationResolver"></bean>
		</property>
	</bean>
```

The following configuration is recommended for annotation-based publishing:

```xml

	<context:component-scan base-package="com.baidu.jprotobuf.pbrpc.spring">
	</context:component-scan>

	<bean
		class="com.baidu.jprotobuf.pbrpc.spring.annotation.CommonAnnotationBeanPostProcessor">
		<property name="callback">
			<bean
				class="com.baidu.jprotobuf.pbrpc.spring.annotation.ProtobufRpcAnnotationResolver"></bean>
		</property>
	</bean>


```

**Export an RPC server service with @RpcExporter<br>**
```java
@Component
@RpcExporter(port = "1031")
public class EchoServiceImpl {
    @ProtobufRPCService(serviceName = "echoService", methodName = "echo")
    public EchoInfo doEcho(EchoInfo info) {
        EchoInfo ret = new EchoInfo();
        ret.setMessage("hello:" + info.getMessage());
        return ret;
    }

```

**Connect a client with @RpcProxy<br>**
```java
@Service("echoServiceClient")
public class AnnotationEchoServiceClient {

    @RpcProxy(port = "1031", host = "127.0.0.1", serviceInterface = EchoService.class, lookupStubOnStartup = false)
    public EchoService echoService;
}

public interface EchoService {
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 1000)
    EchoInfo echo(EchoInfo info);
}
```

**Connect a load-balanced client with @HaRpcProxy<br>**
Note: HaRpcProxy requires a NamingService that provides the server list. With annotations, define it in XML and reference it from the annotation.

```xml
	<bean id="namingService" class="com.baidu.jprotobuf.pbrpc.spring.UrlBasedNamingService">
		<constructor-arg>
			<value>localhost:1031;localhost:1032;localhost:1033</value>
		</constructor-arg>
	</bean>
```

```java
@Service("echoServiceClient")
public class AnnotationEchoServiceClient {

    @HaRpcProxy(namingServiceBeanName = "namingService", serviceInterface = EchoService.class,
            lookupStubOnStartup = false)
    public EchoService haEchoService;
}

public interface EchoService {
    @ProtobufRPC(serviceName = "echoService", onceTalkTimeout = 1000)
    EchoInfo echo(EchoInfo info);
}
```

#### Redis Service Registry ####
jprotobuf-rpc supports service registration and discovery based on Redis.

Example configuration:
```xml
    <bean id="namingService" class="com.baidu.pbrpc.register.redis.RedisRegistryService">
       <constructor-arg>
           <bean class="com.baidu.pbrpc.register.redis.RedisClient">
              <property name="redisServer" value="localhost"></property>
              <property name="port" value="6379"></property>
              <property name="testOnBorrow" value="true"></property>
              <property name="maxWait" value="2000"></property>
           </bean>
       </constructor-arg>
       <property name="administrator" value="true"></property>
       <property name="group" value="default/"></property>
       <property name="expirePeriod" value="3000"></property>
    </bean>

```
RedisRegistryService properties:
1.	`expirePeriod`: service expiration time. After a service registers, its last registration time is written, and its liveness is refreshed periodically (every expirePeriod/3).
2.	`administrator`: defaults to false. When set to true, expired services registered in Redis are deleted. Usually one server doing the cleanup is enough, but multiple servers can clean up together.
3.	`group`: the group name. Only services in the same group can discover each other.


Publishing a service:
```xml
  <bean class="com.baidu.jprotobuf.pbrpc.spring.RpcServiceExporter">
        <property name="servicePort" value="1031"></property>
        <property name="registerServices">
            <list>
                <ref local="echoService" />
            </list>
        </property>
        <property name="registryCenterService" ref="namingService"></property>
        <property name="connectTimeout" value="1000"></property>
    </bean>

```

Client:
```xml
<bean id="echoServiceProxy" class="com.baidu.jprotobuf.pbrpc.spring.HaRpcProxyFactoryBean">
        <property name="serviceInterface" value="com.baidu.jprotobuf.pbrpc.EchoService"></property>
        <property name="namingService" ref="namingService"></property>
    </bean>

```

#### HTTP Management ####
Supported since 3.1.1. It can be configured as follows:
1.       In code
```java 
   RpcServerOptions rpcServerOptions = new RpcServerOptions();
   rpcServerOptions.setHttpServerPort(8866);
       
    RpcServer rpcServer = new RpcServer(rpcServerOptions);
 ```
2.       Spring XML
```xml 
    <bean class="com.baidu.jprotobuf.pbrpc.spring.RpcServiceExporter">
        <property name="servicePort" value="1031"></property>
        <property name="registerServices">
            <list>
                <ref local="echoService" />
            </list>
        </property>
        <property name="connectTimeout" value="1000"></property>
        <property name="httpServerPort" value="8866"></property>
    </bean>
 ```
 
3.       Spring annotations
```java
@RpcExporter(port = "1033" , rpcServerOptionsBeanName = "rpcServerOptions")
public class AnnotationEchoServiceImpl3 extends EchoServiceImpl {
}
```
```xml
    <bean id="rpcServerOptions" class="com.baidu.jprotobuf.pbrpc.transport.RpcServerOptions">
        <property name="acceptorThreads" value="1"></property>
        <property name="workThreads" value="20"></property>
        <property name="httpServerPort" value="8866"></property>
    </bean>
```


For more examples, see the unit test com.baidu.jprotobuf.pbrpc.spring.AnnotationRpcXmlConfigurationTest.
