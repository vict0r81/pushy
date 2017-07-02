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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.OrderedEventExecutor;
import io.netty.util.concurrent.Promise;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

class ApnsConnectionPool {

    private final OrderedEventExecutor executor;
    private final int capacity;

    private final Set<Channel> allChannels = new HashSet<>();
    private final Queue<Channel> idleChannels = new ArrayDeque<>();

    private final Queue<Promise<Channel>> pendingAcquisitionPromises = new ArrayDeque<>();

    public ApnsConnectionPool(final OrderedEventExecutor executor, final int capacity) {
        this.executor = executor;
        this.capacity = capacity;
    }

    public Future<Channel> acquire(final Promise<Channel> promise) {
        if (this.executor.inEventLoop()) {
            this.acquireWithinEventExecutor(promise);
        } else {
            this.executor.submit(new Runnable() {
                @Override
                public void run() {
                    ApnsConnectionPool.this.acquireWithinEventExecutor(promise);
                }
            });
        }

        return promise;
    }

    private void acquireWithinEventExecutor(final Promise<Channel> promise) {
        assert this.executor.inEventLoop();

        final Channel channelFromIdlePool = ApnsConnectionPool.this.idleChannels.poll();

        if (channelFromIdlePool != null) {
            if (channelFromIdlePool.isActive()) {
                promise.trySuccess(channelFromIdlePool);
            } else {
                this.discardChannel(channelFromIdlePool);
                this.acquireWithinEventExecutor(promise);
            }
        } else {
            // We don't have any connections ready to go; create a new one if possible.
            if (allChannels.size() < this.capacity) {
                // TODO
            } else {
                pendingAcquisitionPromises.add(promise);
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
                    ApnsConnectionPool.this.releaseWithinEventExecutor(channel);
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
}
