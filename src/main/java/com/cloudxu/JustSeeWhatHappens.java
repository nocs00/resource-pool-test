package com.cloudxu;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JustSeeWhatHappens {

  public static void main(String[] args) throws Exception {
    ExecutorService exec = Executors.newCachedThreadPool();
    ResourcePool<MyResource> pool = new ResourcePoolImpl<>();
    exec.submit(() -> pool.acquire());
    for (int i = 0; i < 10; i++) {
      int id = i+1;
      exec.submit(() -> pool.add(new MyResource(id)));
    }


    exec.submit(() -> pool.open());
    exec.submit(() -> pool.remove(new MyResource(5)));
    exec.submit(() -> pool.remove(new MyResource(6)));
    exec.submit(() -> log.info("pool is open? [{}]", pool.isOpen()));

    for (int i = 0; i < 20; i++) {
      CompletableFuture<MyResource> future =
          CompletableFuture.supplyAsync(() -> pool.acquire(), exec);

      future.thenApply(res -> {
        log.info("got res [{}]", res);
        try {
          Thread.sleep(500L);
        } catch (InterruptedException e) {
          log.error(e.getMessage());
        }
        pool.release(res);
        return res;
      });
    }

    Thread.sleep(1500L);
    pool.close();
    exec.shutdown();
  }

  @EqualsAndHashCode
  public static class MyResource {
    private int id;

    public MyResource(int id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return "res:" + id;
    }
  }

}
