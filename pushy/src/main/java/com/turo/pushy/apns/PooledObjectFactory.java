package com.turo.pushy.apns;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

interface PooledObjectFactory <T> {

    Future<T> create(Promise<T> promise);

    Future<Void> destroy(T object, Promise<Void> promise);
}
