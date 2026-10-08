/*
 * Copyright 2002-2014 the original author or authors.
 *
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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.jprotobuf.pbrpc.data.RpcDataPackage;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.util.Timeout;

/**
 * RPC request and response channel processor.
 * 
 * @author xiemalin
 * @since 1.0
 */
public class RpcChannel {

    /** The log. */
    private static Logger LOG = LoggerFactory.getLogger(RpcChannel.class.getName());

    /** RPC client. */
    private RpcClient rpcClient;

    /** The channel pool. */
    private ChannelPool channelPool;
    
    private Connection connection;

    /**
     * try to do connect.
     */
    public void testChannlConnect() {
        Connection channel = channelPool.getChannel();
        channelPool.returnChannel(channel);
    }

    /**
     * Instantiates a new rpc channel.
     *
     * @param rpcClient the rpc client
     * @param host the host
     * @param port the port
     */
    public RpcChannel(RpcClient rpcClient, String host, int port) {
        this.rpcClient = rpcClient;
        channelPool = new ChannelPool(rpcClient, host, port);
        rpcClient.setChannelPool(channelPool);
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     */
    public Connection getConnection() {
        Connection channel = channelPool.getChannel();
        return channel;
    }
    
    /**
     * Gets the reused connection.
     *
     * @return the reused connection
     */
    public synchronized Connection getReusedConnection() {
        // The pool only screens a connection when it is borrowed, and this one is borrowed once and then kept, so a
        // connection that broke afterwards, its event loop dead for instance, would be reused for good. Every call
        // already hands it back to the pool when it completes, so just forget it here and borrow again: the pool
        // drops the broken one when it is next lent out.
        if (connection != null && !ChannelPoolObjectFactory.isUsable(connection.getFuture().channel())) {
            connection = null;
        }
        if (connection == null) {
            connection = getConnection();
        }
        return connection;
    }

    /**
     * Release connection.
     *
     * @param connection the connection
     */
    public void releaseConnection(Connection connection) {
        channelPool.returnChannel(connection);
    }

    /**
     * Do transport.
     *
     * @param connection the connection
     * @param rpcDataPackage the rpc data package
     * @param callback the callback
     * @param onceTalkTimeout the once talk timeout
     */
    public void doTransport(Connection connection, RpcDataPackage rpcDataPackage,
            BlockingRpcCallback callback, long onceTalkTimeout) {
        if (rpcDataPackage == null) {
            throw new IllegalArgumentException("param 'rpcDataPackage' is null.");
        }

        long callMethodStart = System.currentTimeMillis();

        // register timer
        Timeout timeout = rpcClient.getTimer()
                .newTimeout(new RpcTimerTask(rpcDataPackage.getRpcMeta().getCorrelationId(), this.rpcClient,
                        onceTalkTimeout, TimeUnit.MILLISECONDS), onceTalkTimeout, TimeUnit.MILLISECONDS);

        RpcClientCallState state = new RpcClientCallState(callback, rpcDataPackage, timeout);

        Long correlationId = state.getDataPackage().getRpcMeta().getCorrelationId();
        rpcClient.registerPendingRequest(correlationId, state);
        if (!connection.getFuture().isSuccess()) {
            try {
                connection.produceRequest(state);
            } catch (IllegalStateException e) {
                RpcClientCallState callState = rpcClient.removePendingRequest(correlationId);
                if (callState != null) {
                    callState.handleFailure(e.getMessage());
                    LOG.debug("id:" + correlationId + " is put in the queue");
                }
            }
        } else {
            final Channel channel = connection.getFuture().channel();
            state.setChannel(channel);

            LOG.debug("Do send request with service name '" + rpcDataPackage.serviceName() + "' method name '"
                    + rpcDataPackage.methodName() + "' bound channel =>" + channel);
            // A request that never left must not wait for a response until onceTalkTimeout. Writes on a channel whose
            // event loop has died fail at once, but only through the returned future, and a listener cannot help in
            // that case: netty hands listener notifications to the same dead loop, which rejects them. So check the
            // future right away, where such a failure is already visible, and listen only for later outcomes.
            final ChannelFuture writeFuture;
            try {
                writeFuture = channel.writeAndFlush(state.getDataPackage());
            } catch (RuntimeException e) {
                failUnsentRequest(correlationId, channel, e);
                return;
            }
            if (writeFuture.isDone()) {
                if (!writeFuture.isSuccess()) {
                    failUnsentRequest(correlationId, channel, writeFuture.cause());
                }
            } else {
                writeFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            failUnsentRequest(correlationId, channel, future.cause());
                        }
                    }
                });
            }
        }

        long callMethodEnd = System.currentTimeMillis();
        LOG.debug("profiling callMethod cost " + (callMethodEnd - callMethodStart) + "ms");

    }

    /**
     * Fails a request whose write did not go through, unless a response or the timeout settled it first.
     *
     * @param correlationId the correlation id of the request
     * @param channel the channel the request was written to
     * @param cause why the write failed
     */
    private void failUnsentRequest(Long correlationId, Channel channel, Throwable cause) {
        RpcClientCallState state = rpcClient.removePendingRequest(correlationId);
        if (state != null) {
            String message = "correlationId:" + correlationId + " failed to send request on channel =>" + channel
                    + ": " + cause;
            LOG.warn(message);
            state.handleFailure(message);
        }
    }

    /**
     * Close.
     */
    public void close() {
        if (channelPool != null) {
            channelPool.stop();
        }

    }

}
