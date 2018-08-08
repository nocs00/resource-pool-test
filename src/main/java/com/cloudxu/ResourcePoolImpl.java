package com.cloudxu;

import static java.util.Arrays.asList;

import java.beans.PropertyChangeSupport;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//delegating much of thread-safety questions to ConcurrentHashMap and AtomicBoolean
//awaiting conditions is based on callbacks, logic similar to wait-notify pipeline
public class ResourcePoolImpl<R> implements ResourcePool<R> {

  private static final Runnable noOp = () -> {};

  //sum of those sets are all resources added via add(R) method
  private final Set<R> freeResources = ConcurrentHashMap.newKeySet();
  private final Set<R> acquiredResources = ConcurrentHashMap.newKeySet();

  private volatile AtomicBoolean isPoolOpen = new AtomicBoolean(false);
  //needed for transition resources from free to acquired set and vice versa
  private final Object resourcesMonitor = new Object();

  //no blocking on pool as other threads can call release/add/remove any time
  public void open() {
    log.info("opening pool");
    isPoolOpen.set(true);
  }

  //no blocking as AtomicBoolean is thread-safe itself
  public boolean isOpen() {
    return isPoolOpen.get();
  }

  //setting pool closed to restrict acquire() calls
  //listening 'release' events until all resources are released
  public void close() {
    log.info("closing pool");
    isPoolOpen.set(false);
    while (!acquiredResources.isEmpty()) {
      try {
        blockTillEvent("release", noOp);
      } catch (InterruptedException e) {
        log.error("Failed to close the pool", e);
        throw new RuntimeException("Failed to close the pool", e);
      }
    }
    log.info("all resources released, pool is gracefully closed");
  }

  //just setting flag, this won't accept acquire,
  // but will accept all other according to contract: release, add, remove
  // hence eventually all resources will be released without blocking calling thread
  public void closeNow() {
    log.info("closing pool now");
    isPoolOpen.set(false);
    log.info("pool is closed");
  }

  //blocks until acquired
  public R acquire() {
    return acquire(0L, TimeUnit.MILLISECONDS);
  }

  //awaits callback from release() or add() for 'timeout' if no free resources in the pool
  //blocks shortly to transit resource from free to acquired set
  //if timeout <= 0 it will just block until callback
  public R acquire(long timeout, TimeUnit timeUnit) {
    log.info("trying to acquire resource");

    if (!isPoolOpen.get()) {
      log.error("Pool is closed. Acquire is not allowed");
      throw new IllegalStateException("Pool is closed. Acquire is not allowed");
    }

    R resource = null;
    if (freeResources.isEmpty()) {
      try {
        Long awaitTimeout = timeout != 0L ? timeUnit.toMillis(timeout) : null;
        blockTillAny(asList("release", "add"), noOp, awaitTimeout);
      } catch (InterruptedException e) {
        log.error("Failed to acquire resource", e);
        throw new RuntimeException("Failed to acquire resource", e);
      }
    }

    synchronized (resourcesMonitor) {
      if (!freeResources.isEmpty()) {
        resource = freeResources.iterator().next();
        freeResources.remove(resource);
        acquiredResources.add(resource);
      }
    }

    if (resource == null) {
      log.error("Failed to acquire resource");
      throw new RuntimeException("Failed to acquire resource");
    }
    log.info("acquired [{}]", resource);
    return resource;
  }

  //block shortly to transit from acquired to free set
  //notify all 'release' listeners that added more free resources
  public void release(R resource) {
    log.info("trying to release resource [{}]", resource);
    Objects.requireNonNull(resource);

    //todo: try to avoid sync:
    /*
      if (!acquiredResources.contains(resource)) {
        log.error("resource was not acquired, nothing to release [{}]", resource);
        throw new IllegalStateException("This resource was not acquired before");
      }
      freeResources.add(resource);
      fireEvent("release");
     */

    synchronized (resourcesMonitor) {
      if (!acquiredResources.remove(resource)) {
        log.error("resource was not acquired, so nothing to release [{}]", resource);
        throw new IllegalStateException("nothing to release: " + resource);
      }
      freeResources.add(resource);
    }
    fireEvent("release");
    log.info("released [{}]", resource);
  }

  //delegating thread-safety to ConcurrentHashMap
  //no block
  //notify all 'add' listeners that added more free resources
  public boolean add(R resource) {
    log.info("adding resource to pool [{}]", resource);
    Objects.requireNonNull(resource, "resource must be non-null");

    boolean isAdded = freeResources.add(resource);
    fireEvent("add");
    log.info("added? [{}, {}]", resource, isAdded);
    return isAdded;
  }

  //trying to release resource if it is acquired
  //block only on release time
  public boolean remove(R resource) {
    log.info("removing resource from the pool [{}]", resource);
    Objects.requireNonNull(resource, "resource must be non-null");

    if (acquiredResources.contains(resource)) {
      release(resource);
    }

    boolean removed = freeResources.remove(resource);
    if (!removed) {
      log.info("no such resource in the pool [{}]", resource);
    } else {
      log.info("removed resource [{}]", resource);
    }

    return removed;
  }

  //roughly remove resource from either sets
  //so if happen consecutive release(R) it will just fail that there is nothing to release
  public boolean removeNow(R resource) {
    log.info("removing immediately resource from the pool [{}]", resource);
    Objects.requireNonNull(resource, "resource must be non-null");

    boolean removed = acquiredResources.remove(resource)
        || freeResources.remove(resource);
    log.info("removed? [{}, {}]", resource, removed);
    return removed;
  }

  /**
   * Blocking callback processing logic based on Java's Observer pattern.
   * Imitating PropertyChangeEvent as callback calls.
   * Block current thread via CountDownLatch until other thread trigger callback.
   */

  private final PropertyChangeSupport support = new PropertyChangeSupport(this);

  private void blockTillEvent(String type, Runnable callback, Long timeoutMillis) throws InterruptedException {
    log.info("registered callback for event type [{}]", type);
    CountDownLatch latch = new CountDownLatch(1);
    support.addPropertyChangeListener(type, evt -> {
      log.info("received event of type [{}]", evt.getPropertyName());
      callback.run();
      latch.countDown();
    });

    if (timeoutMillis == null || timeoutMillis <= 0L) {
      latch.await();
    } else {
      boolean isEventReceived = latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
      if (!isEventReceived) {
        log.error("failed awaiting event, time elapsed [{}ms]", timeoutMillis);
        throw new InterruptedException("failed awaiting event, time elapsed(ms): " + timeoutMillis);
      }
    }
  }

  private void blockTillEvent(String type, Runnable callback) throws InterruptedException {
    blockTillEvent(type, callback, null);
  }

  private void blockTillAny(List<String> types, Runnable callback, Long timeoutMillis) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    types.forEach(type -> {
      log.info("registered callback for event type [{}]", type);
      support.addPropertyChangeListener(type, evt -> {
        log.info("received event of type [{}]", evt.getPropertyName());
        callback.run();
        latch.countDown();
      });
    });

    if (timeoutMillis == null || timeoutMillis <= 0L) {
      latch.await();
    } else {
      boolean isEventReceived = latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
      if (!isEventReceived) {
        log.error("failed awaiting event, time elapsed [{}ms]", timeoutMillis);
        throw new InterruptedException("failed awaiting event, time elapsed(ms): " + timeoutMillis);
      }
    }
  }

  private void blockTillAny(List<String> types, Runnable callback) throws InterruptedException {
    blockTillAny(types, callback, null);
  }


  private void fireEvent(String type) {
    log.info("fired event type [{}]", type);
    support.firePropertyChange(type, true, false);
  }

}
