/*
 * Copyright (c) 2013-2017 Turo
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.turo.pushy.apns;

import com.turo.pushy.apns.auth.ApnsSigningKey;
import com.turo.pushy.apns.proxy.ProxyHandlerFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

class ApnsChannelFactory {

    private final Bootstrap bootstrapTemplate;

    private static final AttributeKey<ChannelPromise> CHANNEL_READY_PROMISE_ATTRIBUTE_KEY =
            AttributeKey.valueOf(ApnsChannelFactory.class, "channelReadyPromise");

    public ApnsChannelFactory(final SslContext sslContext, final ApnsSigningKey signingKey,
                              final ProxyHandlerFactory proxyHandlerFactory, final int connectTimeoutMillis,
                              final long idlePingIntervalMillis, final long gracefulShutdownTimeoutMillis,
                              final InetSocketAddress apnsServerAddress, final EventLoopGroup eventLoopGroup) {

        this.bootstrapTemplate = new Bootstrap();
        this.bootstrapTemplate.group(eventLoopGroup);
        this.bootstrapTemplate.channel(SocketChannelClassUtil.getSocketChannelClass(this.bootstrapTemplate.config().group()));
        this.bootstrapTemplate.option(ChannelOption.TCP_NODELAY, true);
        this.bootstrapTemplate.remoteAddress(apnsServerAddress);

        if (connectTimeoutMillis > 0) {
            this.bootstrapTemplate.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
        }

        this.bootstrapTemplate.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(final SocketChannel channel) throws Exception {
                final ChannelPipeline pipeline = channel.pipeline();

                if (proxyHandlerFactory != null) {
                    pipeline.addFirst(proxyHandlerFactory.createProxyHandler());
                }

                final SslHandler sslHandler = sslContext.newHandler(channel.alloc());

                sslHandler.handshakeFuture().addListener(new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(final Future<Channel> future) throws Exception {
                        if (!future.isSuccess()) {
                            channel.attr(CHANNEL_READY_PROMISE_ATTRIBUTE_KEY).get().tryFailure(future.cause());
                        }
                    }
                });

                pipeline.addLast(sslHandler);
                pipeline.addLast(new ApplicationProtocolNegotiationHandler("") {
                    @Override
                    protected void configurePipeline(final ChannelHandlerContext context, final String protocol) {
                        if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                            final ApnsClientHandler apnsClientHandler;

                            final String authority = ((InetSocketAddress) context.channel().remoteAddress()).getHostName();

                            if (signingKey != null) {
                                apnsClientHandler = new TokenAuthenticationApnsClientHandler.TokenAuthenticationApnsClientHandlerBuilder()
                                        .signingKey(signingKey)
                                        .authority(authority)
                                        .idlePingIntervalMillis(idlePingIntervalMillis)
                                        .build();
                            } else {
                                apnsClientHandler = new ApnsClientHandler.ApnsClientHandlerBuilder()
                                        .authority(authority)
                                        .idlePingIntervalMillis(idlePingIntervalMillis)
                                        .build();
                            }

                            if (gracefulShutdownTimeoutMillis > 0) {
                                apnsClientHandler.gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis);
                            }

                            context.pipeline().addLast(new IdleStateHandler(0, 0, idlePingIntervalMillis, TimeUnit.MILLISECONDS));
                            context.pipeline().addLast(apnsClientHandler);

                            channel.attr(CHANNEL_READY_PROMISE_ATTRIBUTE_KEY).get().trySuccess();
                        } else {
                            throw new IllegalArgumentException("Unexpected protocol: " + protocol);
                        }
                    }
                });
            }
        });
    }

    public ChannelFuture createChannel() {
        final ChannelFuture connectFuture = this.bootstrapTemplate.clone().connect();
        final ChannelPromise channelReadyPromise = new DefaultChannelPromise(connectFuture.channel());

        channelReadyPromise.channel().attr(CHANNEL_READY_PROMISE_ATTRIBUTE_KEY).set(channelReadyPromise);

        connectFuture.addListener(new GenericFutureListener<ChannelFuture>() {

            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    // This may seem spurious, but our goal here is to accurately report the cause of
                    // connection failure; if we just wait for connection closure, we won't be able to
                    // tell callers anything more specific about what went wrong.
                    channelReadyPromise.tryFailure(future.cause());
                }
            }
        });

        connectFuture.channel().closeFuture().addListener(new GenericFutureListener<ChannelFuture> () {

            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                // We always want to try to fail the "channel ready" promise if the connection closes; if it has already
                // succeeded, this will have no effect.
                channelReadyPromise.tryFailure(
                        new IllegalStateException("Channel closed before HTTP/2 preface completed."));
            }
        });

        return channelReadyPromise;
    }
}
