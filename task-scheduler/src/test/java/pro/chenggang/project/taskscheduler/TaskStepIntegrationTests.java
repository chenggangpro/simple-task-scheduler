
package pro.chenggang.project.taskscheduler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
@Order(Integer.MAX_VALUE)
public class TaskStepIntegrationTests {

    @Test
    void testConcurrency() throws Exception {
        int round = 3;
        CountDownLatch countDownLatch = new CountDownLatch(round);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime()
                .availableProcessors());
        for (int i = 0; i < round; i++) {
            scheduledExecutorService.submit(() -> {
                try {
                    simpleRun();
                } catch (Exception e) {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Result Error:" + e.getMessage());
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        countDownLatch.await();
        scheduledExecutorService.shutdown();
    }

    @Test
    void simpleRun() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadScheduledExecutor(new NameThreadFactory("STEP"));
        try {
            TaskStep.newTaskStep(
                            () -> {
                                System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 1");
                                try {
                                    TimeUnit.SECONDS.sleep(1);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 2");
                                return Optional.of(String.valueOf(new Random().nextInt(14)));
                            },
                            singleThreadExecutor
                    )
                    .stepExecution()
                    .convert(value -> {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        String stringValue = value + new Random().nextInt(14);
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                        return stringValue;
                    })
                    .filter(value -> {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                        return Integer.parseInt(value) % 2 == 0;
                    })
                    .validate(value -> {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                        if (Integer.parseInt(value) % 4 == 0) {
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                            return Optional.of(new RuntimeException(Thread.currentThread()
                                    .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                        }
                        return Optional.empty();
                    })
                    .ifEmptyThen(() -> Optional.of("-1"))
                    .transform(value -> () -> {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                        return Optional.of(Integer.parseInt(value));
                    })
                    .toRunner()
                    .run()
                    .ifPresentOrElse(
                            result -> System.out.println(Thread.currentThread().getName() + " ==> " + "Result:" + result),
                            () -> {
                                System.out.println(Thread.currentThread().getName() + " ==> " + "Result is empty");
                            }
                    );
        } catch (RuntimeException e) {
            Assertions.assertNotNull(e);
            Assertions.assertInstanceOf(e.getClass(), new RuntimeException());
        }
        singleThreadExecutor.shutdown();
    }

    @Test
    void simpleRunAndHandleException() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadScheduledExecutor(new NameThreadFactory("STEP"));
        TaskStep.newTaskStep(
                        () -> {
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor
                )
                .stepExecution()
                .convert(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) > 0) {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .defaultWhenError(throwable -> {
                    if(throwable instanceof IllegalStateException ){
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Default when error : " + throwable);
                        return Optional.of(-2);
                    }
                    throw new CompletionException(throwable);
                })
                .toRunner(throwable -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Runner error:" + throwable);
                })
                .run()
                .ifPresentOrElse(
                        result -> System.out.println(Thread.currentThread().getName() + " ==> " + "Result:" + result),
                        () -> {
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Result is empty");
                        }
                );
        singleThreadExecutor.shutdown();
    }

    @Test
    void noRun() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new NameThreadFactory("STEP"));
        StepExecutionRunner<Integer> runner = TaskStep.newTaskStep(
                        () -> {
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor
                )
                .stepExecution()
                .convert(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) % 4 == 0) {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .toRunner();
        runner.cancel();
//        runner.run();
        System.out.println(singleThreadExecutor);
        singleThreadExecutor.shutdown();
    }

    @Test
    void simpleRunWithDefaultExecutor() { // TODO
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new NameThreadFactory("STEP"));
        StepExecutionRunner<Integer> runner = TaskStep.newTaskStep(
                        () -> {
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            System.out.println(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor
                )
                .stepExecution()
                .convert(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) % 4 == 0) {
                        System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .toRunner();
        runner.cancel();
//        runner.run();
        System.out.println(singleThreadExecutor);
        singleThreadExecutor.shutdown();
    }

    class NameThreadFactory implements ThreadFactory {

        private final AtomicInteger id = new AtomicInteger(0);

        private String name;

        public NameThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            String threadName = name + '-' + id.getAndDecrement();
            Thread thread = new Thread(r, threadName);
            thread.setDaemon(true);
            return thread;
        }
    }
}
