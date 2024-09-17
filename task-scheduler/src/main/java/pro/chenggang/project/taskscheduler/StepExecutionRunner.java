package pro.chenggang.project.taskscheduler;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static pro.chenggang.project.taskscheduler.TaskStep.executorToString;

/**
 * The Step execution runner.
 *
 * @param <T> the data type
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
@Slf4j
public class StepExecutionRunner<T> {

    /**
     * Whether all steps have been already executed or canceled
     */
    private final AtomicBoolean alreadyExecuted = new AtomicBoolean(false);

    /**
     * The task step info
     */
    private final TaskStepInfo taskStepInfo;

    /**
     * The Start point.
     */
    private final CompletableFuture<Void> startPoint;

    /**
     * The source future.
     */
    private final CompletableFuture<Optional<T>> sourceFuture;

    /**
     * The Current executor.
     */
    private final Executor currentExecutor;

    /**
     * The exception handler
     */
    private final Consumer<Throwable> exceptionHandler;


    /**
     * Instantiates a new Step execution runner.
     *
     * @param taskStepInfo     the task step info
     * @param startPoint       the start point
     * @param sourceFuture     the source future
     * @param executor         the executor
     * @param exceptionHandler the exception handler
     */
    protected StepExecutionRunner(@NonNull TaskStepInfo taskStepInfo,
                                  @NonNull CompletableFuture<Void> startPoint,
                                  @NonNull CompletableFuture<Optional<T>> sourceFuture,
                                  Executor executor,
                                  Consumer<Throwable> exceptionHandler) {
        this.taskStepInfo = taskStepInfo;
        this.startPoint = startPoint;
        this.sourceFuture = sourceFuture;
        this.currentExecutor = Objects.nonNull(executor) ? executor : sourceFuture.defaultExecutor();
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Gets start point
     *
     * @return the start point CompletableFuture
     */
    protected CompletableFuture<Void> startPoint() {
        return startPoint;
    }

    /**
     * Over all completable future.
     *
     * @return the completable future
     */
    protected CompletableFuture<Optional<T>> overAll() {
        return startPoint()
                .thenCompose(aVoid -> {
                    if (!alreadyExecuted.compareAndSet(false, true)) {
                        throw new IllegalStateException("[" + this.taskStepInfo.getFullName() + "]All steps have already been executed or been canceled");
                    }
                    return CompletableFuture.runAsync(this.taskStepInfo::start, this.currentExecutor);
                })
                .thenCompose(aVoid -> Optional.ofNullable(this.exceptionHandler)
                        .map(handler -> this.sourceFuture
                                .handleAsync(
                                        (completedValue, throwable) -> {
                                            if (Objects.nonNull(throwable)) {
                                                this.forceCancelStartPoint();
                                                if (throwable instanceof CompletionException completionException) {
                                                    throwable = completionException.getCause();
                                                }
                                                handler.accept(throwable);
                                                return Optional.<T>empty();
                                            }
                                            return completedValue;
                                        },
                                        currentExecutor
                                )
                        )
                        .orElse(this.sourceFuture)
                )
                .handleAsync((completedValue, throwable) -> {
                    this.taskStepInfo.stop();
                    if (Objects.nonNull(throwable)) {
                        this.forceCancelStartPoint();
                        if (throwable instanceof CompletionException) {
                            Throwable cause = throwable.getCause();
                            if (cause instanceof RuntimeException) {
                                throw (RuntimeException) cause;
                            }
                            throw (CompletionException) throwable;
                        }
                        throw new CompletionException(throwable);
                    }
                    return completedValue;
                });
    }

    /**
     * Force cancel start point
     */
    private void forceCancelStartPoint() {
        if (!this.startPoint.isDone() || !this.startPoint.isCancelled() || !this.startPoint.isCompletedExceptionally()) {
            log.debug("StepExecutionRunner::ForceCancelStartPoint => Task:{},Executor:{}",
                    taskStepInfo.getFullName(),
                    executorToString(this.currentExecutor)
            );
            this.taskStepInfo.canceled();
            this.startPoint.cancel(true);
        }
    }

}
