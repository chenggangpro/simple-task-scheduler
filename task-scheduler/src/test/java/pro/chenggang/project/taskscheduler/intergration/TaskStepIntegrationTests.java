
package pro.chenggang.project.taskscheduler.intergration;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import pro.chenggang.project.taskscheduler.Task;
import pro.chenggang.project.taskscheduler.TaskInfo;
import pro.chenggang.project.taskscheduler.TaskStep;

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
@Slf4j
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
                    log.info(Thread.currentThread().getName() + " ==> " + "Result Error:" + e.getMessage());
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
            Task.newTask("simpleRun")
                    .newTaskStep(
                            "Step1",
                            () -> {
                                log.info(Thread.currentThread().getName() + " ==> " + "Step: 1");
                                try {
                                    TimeUnit.SECONDS.sleep(1);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                log.info(Thread.currentThread().getName() + " ==> " + "Step: 2");
                                return Optional.of(String.valueOf(new Random().nextInt(14)));
                            },
                            singleThreadExecutor
                    )
                    .stepExecution()
                    .convert(value -> {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        String stringValue = value + new Random().nextInt(14);
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                        return stringValue;
                    })
                    .filter(value -> {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                        return Integer.parseInt(value) % 2 == 0;
                    })
                    .validate(value -> {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                        if (Integer.parseInt(value) % 4 == 0) {
                            log.info(Thread.currentThread()
                                    .getName() + " ==> " + "Step: Validate with error");
                            return Optional.of(new RuntimeException(Thread.currentThread()
                                    .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                        }
                        return Optional.empty();
                    })
                    .ifEmptyThen(() -> Optional.of("-1"))
                    .transform(value -> () -> {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                        return Optional.of(Integer.parseInt(value));
                    })
                    .endTaskStep()
                    .runAllSteps()
                    .ifPresentOrElse(
                            result -> log.info(Thread.currentThread()
                                    .getName() + " ==> " + "Result:" + result),
                            () -> {
                                log.info(Thread.currentThread().getName() + " ==> " + "Result is empty");
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
        Task.newTask("simpleRunAndHandleException")
                .newTaskStep(
                        "Step1",
                        () -> {
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor
                )
                .stepExecution()
                .convert(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) > 0) {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .defaultWhenError(throwable -> {
                    if (throwable instanceof IllegalStateException) {
                        log.info(Thread.currentThread()
                                .getName() + " ==> " + "Step: Default when error : " + throwable);
                        return Optional.of(-2);
                    }
                    throw new CompletionException(throwable);
                })
                .exceptionallyThenEndTaskStep(throwable -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Runner error:" + throwable);
                })
                .runAllSteps()
                .ifPresentOrElse(
                        result -> log.info(Thread.currentThread().getName() + " ==> " + "Result:" + result),
                        () -> {
                            log.info(Thread.currentThread().getName() + " ==> " + "Result is empty");
                        }
                );
        singleThreadExecutor.shutdown();
    }

    @Test
    void noRun() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new NameThreadFactory("STEP"));
        TaskInfo taskInfo = Task.newTask("noRun")
                .newTaskStep(
                        "Step1",
                        () -> {
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor
                )
                .stepExecution()
                .convert(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) % 4 == 0) {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .endTaskStep()
                .cancelAllSteps();
        taskInfo.getTaskStepInfos().forEach(taskStepInfo -> System.out.println(taskStepInfo.summary()));
        singleThreadExecutor.shutdown();
    }

    @Test
    void simpleRunWithDefaultExecutor() { // TODO
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new NameThreadFactory("STEP"));
        TaskInfo taskInfo = Task.newTask("simpleRunWithDefaultExecutor")
                .newTaskStep(
                        "Step1",
                        () -> {
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor
                )
                .stepExecution()
                .convert(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) % 4 == 0) {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .endTaskStep()
                .cancelAllSteps();
        taskInfo.getTaskStepInfos().forEach(taskStepInfo -> System.out.println(taskStepInfo.summary()));
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

    @Test
    void testMultipleSteps() {
        ExecutorService singleThreadExecutor1 = Executors.newSingleThreadExecutor(new NameThreadFactory("STEP1"));
        ExecutorService singleThreadExecutor2 = Executors.newSingleThreadExecutor(new NameThreadFactory("STEP2"));
        TaskStep<Integer, Integer> taskStep = Task.newTask("testMultipleSteps")
                .newTaskStep(
                        "Step1",
                        () -> {
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 1");
                            try {
                                TimeUnit.SECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            log.info(Thread.currentThread().getName() + " ==> " + "Step: 2");
                            return Optional.of(String.valueOf(new Random().nextInt(14)));
                        },
                        singleThreadExecutor1
                )
                .stepExecution()
                .convert(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String stringValue = value + new Random().nextInt(14);
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Convert 2");
                    return stringValue;
                })
                .filter(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return Integer.parseInt(value) % 2 == 0;
                })
                .validate(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate");
                    if (Integer.parseInt(value) % 4 == 0) {
                        log.info(Thread.currentThread().getName() + " ==> " + "Step: Validate with error");
                        return Optional.of(new RuntimeException(Thread.currentThread()
                                .getName() + " ==> " + "Mod 4 is zero, value :" + value));
                    }
                    return Optional.empty();
                })
                .ifEmptyThen(() -> Optional.of("-1"))
                .transform(value -> () -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Transform 2");
                    return Optional.of(Integer.parseInt(value));
                })
                .exceptionallyThenNextTaskStep("Step2",
                        throwable -> {
                            log.info(Thread.currentThread().getName() + " ==> " + "Runner error:" + throwable);
                        },
                        singleThreadExecutor2
                )
                .stepExecution()
                .filter(value -> {
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(Thread.currentThread().getName() + " ==> " + "Step: Filter 2");
                    return value % 3 == 0;
                })
                .ifEmptyThen(() -> Optional.of(-1))
                .endTaskStep();
        taskStep.runAllSteps();
        taskStep.getTaskInfo().getTaskStepInfos().forEach(taskStepInfo -> System.out.println(taskStepInfo.summary()));
        singleThreadExecutor1.shutdown();
        singleThreadExecutor2.shutdown();
    }
}
