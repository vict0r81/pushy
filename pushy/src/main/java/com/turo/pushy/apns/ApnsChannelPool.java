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
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

class ApnsChannelPool {

    private final PooledObjectFactory<Channel> channelFactory;
    private final OrderedEventExecutor executor;
    private final int capacity;

    private final ApnsChannelPoolMetricsListener metricsListener;

    private final ChannelGroup allChannels;
    private final Queue<Channel> idleChannels = new ArrayDeque<>();

    private final Set<Future<Channel>> pendingCreateChannelFutures = new HashSet<>();
    private final Queue<Promise<Channel>> pendingAcquisitionPromises = new ArrayDeque<>();

    private static final Logger log = LoggerFactory.getLogger(ApnsChannelPool.class);

    private static class NoopChannelPoolMetricsListener implements ApnsChannelPoolMetricsListener {

        @Override
        public void handleConnectionAdded() {
        }

        @Override
        public void handleConnectionRemoved() {
        }

        @Override
        public void handleConnectionCreationFailed() {
        }
    }

    public ApnsChannelPool(final PooledObjectFactory<Channel> channelFactory, final int capacity, final OrderedEventExecutor executor, final ApnsChannelPoolMetricsListener metricsListener) {
        this.channelFactory = channelFactory;
        this.capacity = capacity;
        this.executor = executor;

        this.metricsListener = metricsListener != null ? metricsListener : new NoopChannelPoolMetricsListener();

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
            if (this.allChannels.size() + this.pendingCreateChannelFutures.size() < this.capacity) {
                final Future<Channel> createChannelFuture = this.channelFactory.create(executor.<Channel>newPromise());
                this.pendingCreateChannelFutures.add(createChannelFuture);

                createChannelFuture.addListener(new GenericFutureListener<Future<Channel>>() {

                    @Override
                    public void operationComplete(final Future<Channel> future) throws Exception {
                        ApnsChannelPool.this.pendingCreateChannelFutures.remove(createChannelFuture);

                        if (future.isSuccess()) {
                            final Channel channel = future.getNow();

                            ApnsChannelPool.this.allChannels.add(channel);
                            ApnsChannelPool.this.metricsListener.handleConnectionAdded();

                            acquirePromise.trySuccess(channel);
                        } else {
                            ApnsChannelPool.this.metricsListener.handleConnectionCreationFailed();

                            acquirePromise.tryFailure(future.cause());
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

        this.metricsListener.handleConnectionRemoved();

        this.channelFactory.destroy(channel, this.executor.<Void>newPromise());
    }

    public Future<Void> close() {
        return this.allChannels.close();
    }
}
