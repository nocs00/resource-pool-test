package com.cloudxu;

import java.util.concurrent.TimeUnit;

public interface ResourcePool<R> {

  void open();

  boolean isOpen();

  void close();

  R acquire();

  R acquire(long timeout, TimeUnit timeUnit);

  void release(R resource);

  boolean add(R resource);

  boolean remove(R resource);

  boolean removeNow(R resource);

  void closeNow();

}
