/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.baidu.jprotobuf.pbrpc.transport;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import com.baidu.jprotobuf.pbrpc.ProtobufRPC;
import com.baidu.jprotobuf.pbrpc.ProtobufRPCService;
import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.nio.NioTask;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory.EventExecutorChooser;

/**
 * An event loop of the client dies the way an OutOfMemoryError kills it in production, and the client must keep
 * serving calls instead of letting each of them wait out its timeout on a connection nobody reads any more.
 */
public class DeadEventLoopTest {

    private static final long ONCE_TALK_TIMEOUT_MS = 5000;

    /** Anything well below the timeout proves the call did not wait for it. */
    private static final long FAST_MS = ONCE_TALK_TIMEOUT_MS / 2;

    @ProtobufClass
    public static class Echo {
        public String text;
    }

    public interface EchoService {
        @ProtobufRPC(serviceName = "EchoService", methodName = "echo", onceTalkTimeout = ONCE_TALK_TIMEOUT_MS)
        Echo echo(Echo request);
    }

    public static class EchoServiceImpl {
        @ProtobufRPCService(serviceName = "EchoService", methodName = "echo")
        public Echo echo(Echo request) {
            return request;
        }
    }

    private static RpcServer server;
    private static int port;

    @BeforeClass
    public static void startServer() throws Exception {
        ServerSocket socket = new ServerSocket(0);
        port = socket.getLocalPort();
        socket.close();
        server = new RpcServer();
        server.registerService(new EchoServiceImpl());
        server.start(port);
    }

    @AfterClass
    public static void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    private static RpcClient newClient(int eventLoops, boolean testOnBorrow) {
        return newClient(eventLoops, testOnBorrow, true);
    }

    private static RpcClient newClient(int eventLoops, boolean testOnBorrow, boolean innerReusePool) {
        RpcClientOptions options = new RpcClientOptions();
        options.setInnerResuePool(innerReusePool);
        // Each client gets its own pool. The shared one is keyed by host and port only, so it would carry
        // connections from one test over to the next.
        options.setShareChannelPool(false);
        options.setWorkGroupThreadSize(eventLoops);
        options.setTestOnBorrow(testOnBorrow);
        return new RpcClient(options);
    }

    private static EchoService stub(RpcClient client) {
        ProtobufRpcProxy<EchoService> proxy = new ProtobufRpcProxy<EchoService>(client, EchoService.class);
        proxy.setHost("127.0.0.1");
        proxy.setPort(port);
        return proxy.proxy();
    }

    private static Echo echo(String text) {
        Echo echo = new Echo();
        echo.text = text;
        return echo;
    }

    /**
     * Kills one event loop through netty's public API: an Error thrown from NioTask.channelReady() escapes into
     * NioEventLoop.run(), which rethrows it and lets the thread end, as an OutOfMemoryError does. Netty does that since
     * 4.1.54; older versions log every Throwable and keep the loop running.
     */
    private static void kill(EventExecutor executor) throws Exception {
        NioEventLoop loop = (NioEventLoop) executor;
        Pipe pipe = Pipe.open();
        pipe.source().configureBlocking(false);
        loop.register(pipe.source(), SelectionKey.OP_READ, new NioTask<SelectableChannel>() {
            @Override
            public void channelReady(SelectableChannel ch, SelectionKey key) {
                throw new OutOfMemoryError("injected by DeadEventLoopTest");
            }

            @Override
            public void channelUnregistered(SelectableChannel ch, Throwable cause) {
            }
        });
        pipe.sink().write(ByteBuffer.wrap(new byte[] {1}));
        Assert.assertTrue(loop.terminationFuture().await(10, TimeUnit.SECONDS));
    }

    private static long millisSince(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    @Test
    public void testCallsRecoverAfterTheirEventLoopDies() throws Exception {
        RpcClient client = newClient(2, true);
        try {
            EchoService service = stub(client);
            // The pool's only connection lands on the first loop the chooser hands out.
            Assert.assertEquals("warm", service.echo(echo("warm")).text);

            kill(client.config().group().iterator().next());

            for (int i = 0; i < 3; i++) {
                String text = "after-" + i;
                long start = System.nanoTime();
                Assert.assertEquals(text, service.echo(echo(text)).text);
                long elapsed = millisSince(start);
                Assert.assertTrue("call " + i + " took " + elapsed + " ms", elapsed < FAST_MS);
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testReusedConnectionRecoversAfterItsEventLoopDies() throws Exception {
        // Without the inner reuse pool every call goes through one cached connection, which the pool never screens
        // again once it has been lent out.
        RpcClient client = newClient(2, true, false);
        try {
            EchoService service = stub(client);
            Assert.assertEquals("warm", service.echo(echo("warm")).text);

            kill(client.config().group().iterator().next());

            for (int i = 0; i < 3; i++) {
                String text = "after-" + i;
                long start = System.nanoTime();
                Assert.assertEquals(text, service.echo(echo(text)).text);
                long elapsed = millisSince(start);
                Assert.assertTrue("call " + i + " took " + elapsed + " ms", elapsed < FAST_MS);
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testWriteOnDeadEventLoopFailsFast() throws Exception {
        // Keep the pool from screening the dead connection out, so that the call really writes to it.
        RpcClient client = newClient(1, false);
        try {
            EchoService service = stub(client);
            Assert.assertEquals("warm", service.echo(echo("warm")).text);

            kill(client.config().group().iterator().next());

            for (int i = 0; i < 3; i++) {
                long start = System.nanoTime();
                try {
                    service.echo(echo("lost-" + i));
                    Assert.fail("call " + i + " succeeded on a dead event loop");
                } catch (Exception e) {
                    String chain = causeChain(e);
                    long elapsed = millisSince(start);
                    Assert.assertTrue("call " + i + " took " + elapsed + " ms: " + chain, elapsed < FAST_MS);
                    Assert.assertTrue(chain, chain.contains("failed to send request"));
                }
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testChooserSkipsDeadEventLoops() throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup(3);
        try {
            EventExecutor[] executors = new EventExecutor[3];
            Iterator<EventExecutor> iterator = group.iterator();
            for (int i = 0; i < executors.length; i++) {
                executors[i] = iterator.next();
            }
            EventExecutorChooser chooser = LiveEventExecutorChooserFactory.INSTANCE.newChooser(executors);

            Set<EventExecutor> chosen = new HashSet<EventExecutor>();
            for (int i = 0; i < 6; i++) {
                chosen.add(chooser.next());
            }
            Assert.assertEquals("round robin over all live loops", 3, chosen.size());

            kill(executors[1]);
            for (int i = 0; i < 6; i++) {
                Assert.assertNotSame(executors[1], chooser.next());
            }

            kill(executors[0]);
            kill(executors[2]);
            // Nothing is left to skip to; the chooser still answers rather than spinning.
            Assert.assertNotNull(chooser.next());
        } finally {
            shutdown(group);
        }
    }

    @Test
    public void testChannelOfDeadEventLoopIsClosed() throws Exception {
        NioEventLoopGroup serverGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup clientGroup = new NioEventLoopGroup(1);
        try {
            final CountDownLatch serverSawClose = new CountDownLatch(1);
            Channel serverChannel = new ServerBootstrap().group(serverGroup).channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInboundHandlerAdapter() {
                        @Override
                        public void channelInactive(ChannelHandlerContext ctx) {
                            serverSawClose.countDown();
                        }

                        @Override
                        public boolean isSharable() {
                            return true;
                        }
                    }).bind("127.0.0.1", 0).sync().channel();
            Channel channel = new Bootstrap().group(clientGroup).channel(NioSocketChannel.class)
                    .handler(new ChannelInboundHandlerAdapter())
                    .connect(serverChannel.localAddress()).sync().channel();

            kill(channel.eventLoop());

            // A plain close() has to run on the dead loop, is rejected, and leaves the socket open.
            Assert.assertFalse(channel.close().awaitUninterruptibly().isSuccess());
            Assert.assertTrue(channel.isOpen());

            ChannelPoolObjectFactory.close(channel);
            Assert.assertFalse(channel.isOpen());
            Assert.assertTrue(serverSawClose.await(10, TimeUnit.SECONDS));
        } finally {
            shutdown(serverGroup);
            shutdown(clientGroup);
        }
    }

    private static void shutdown(EventLoopGroup group) {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private static String causeChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            sb.append(c).append(" <- ");
        }
        return sb.toString();
    }
}
