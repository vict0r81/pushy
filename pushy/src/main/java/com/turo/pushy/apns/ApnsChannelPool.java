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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;

class ApnsChannelPool {

    private final ApnsChannelFactory channelFactory;
    private final OrderedEventExecutor executor;
    private final int capacity;

    private final ChannelGroup allChannels;
    private final Queue<Channel> idleChannels = new ArrayDeque<>();

    private final Queue<Promise<Channel>> pendingAcquisitionPromises = new ArrayDeque<>();

    private static final Logger log = LoggerFactory.getLogger(ApnsChannelPool.class);

    public ApnsChannelPool(final ApnsChannelFactory channelFactory, final int capacity, final OrderedEventExecutor executor) {
        this.channelFactory = channelFactory;
        this.capacity = capacity;
        this.executor = executor;

        this.allChannels = new DefaultChannelGroup(this.executor, true);
    }

    public Future<Channel> acquire() {
        final Promise<Channel> acquirePromise = new DefaultPromise<>(this.executor);

        if (this.executor.inEventLoop()) {
            this.acquireWithinEventExecutor(acquirePromise);
        } else {
            this.executor.submit(new Runnable() {
                @Override
                public void run() {
                    ApnsChannelPool.this.acquireWithinEventExecutor(acquirePromise);
                }
            });
        }

        return acquirePromise;
    }

    private void acquireWithinEventExecutor(final Promise<Channel> acquirePromise) {
        assert this.executor.inEventLoop();

        final Channel channelFromIdlePool = ApnsChannelPool.this.idleChannels.poll();

        if (channelFromIdlePool != null) {
            if (channelFromIdlePool.isActive()) {
                acquirePromise.trySuccess(channelFromIdlePool);
            } else {
                this.discardChannel(channelFromIdlePool);
                this.acquireWithinEventExecutor(acquirePromise);
            }
        } else {
            // We don't have any connections ready to go; create a new one if possible.
            if (allChannels.size() < this.capacity) {
                final ChannelFuture createChannelFuture = this.channelFactory.createChannel();
                allChannels.add(createChannelFuture.channel());

                createChannelFuture.addListener(new GenericFutureListener<ChannelFuture>() {

                    @Override
                    public void operationComplete(final ChannelFuture future) throws Exception {
                        // TODO Metrics
                        if (future.isSuccess()) {
                            acquirePromise.trySuccess(future.channel());
                        } else {
                            acquirePromise.tryFailure(future.cause());
                            ApnsChannelPool.this.allChannels.remove(future.channel());
                        }
                    }
                });
            } else {
                pendingAcquisitionPromises.add(acquirePromise);
            }
        }
    }

    public void release(final Channel channel) {
        if (this.executor.inEventLoop()) {
            this.releaseWithinEventExecutor(channel);
        } else {
            this.executor.submit(new Runnable() {
                @Override
                public void run() {
                    ApnsChannelPool.this.releaseWithinEventExecutor(channel);
                }
            });
        }
    }

    private void releaseWithinEventExecutor(final Channel channel) {
        assert this.executor.inEventLoop();

        this.idleChannels.add(channel);

        if (!this.pendingAcquisitionPromises.isEmpty()) {
            this.acquireWithinEventExecutor(this.pendingAcquisitionPromises.poll());
        }
    }

    private void discardChannel(final Channel channel) {
        assert this.executor.inEventLoop();

        this.idleChannels.remove(channel);
        this.allChannels.remove(channel);

        channel.close();
    }

    public Future<Void> close() {
        return this.allChannels.close();
    }
}
