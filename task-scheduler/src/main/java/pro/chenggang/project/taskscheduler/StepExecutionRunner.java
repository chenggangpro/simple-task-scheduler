package pro.chenggang.project.taskscheduler;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The Step execution runner.
 *
 * @param <T> the data type
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
public class StepExecutionRunner<T> {

    /**
     * Whether all steps have been already executed or canceled
     */
    private final AtomicBoolean alreadyExecuted = new AtomicBoolean(false);
    /**
     * The Start point.
     */
    private final CompletableFuture<Void> startPoint;

    /**
     * The Source.
     */
    private final CompletableFuture<Optional<T>> source;

    /**
     * The Current executor.
     */
    private final Executor currentExecutor;

    /**
     * The exception handler
     */
    private final Consumer<Throwable> exceptionHandler;

    /**
     * Instantiates a new Step execution.
     *
     * @param startPoint       the start point
     * @param source           the source future
     * @param executor         the target executor
     * @param exceptionHandler the exception handler
     */
    protected StepExecutionRunner(CompletableFuture<Void> startPoint,
                                  CompletableFuture<Optional<T>> source,
                                  Executor executor,
                                  Consumer<Throwable> exceptionHandler) {
        this.startPoint = startPoint;
        this.source = source;
        this.currentExecutor = executor;
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Run all steps in current task.
     *
     * @return the optional result
     */
    public Optional<T> run() {
        if (!alreadyExecuted.compareAndSet(false, true)) {
            throw new IllegalStateException("All steps have already been executed or been canceled");
        }
        final CompletableFuture<Optional<T>> overall = Optional.ofNullable(this.exceptionHandler)
                .map(handler -> this.source
                        .handleAsync(
                                (completedValue, throwable) -> {
                                    if (Objects.nonNull(throwable)) {
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
                .orElse(this.source);
        try {
            this.startPoint.complete(null);
            return overall.join();
        } catch (CompletionException completionException) {
            Throwable cause = completionException.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw completionException;
        }
    }

    /**
     * Cancel all steps.
     */
    public void cancel() {
        if (!alreadyExecuted.compareAndSet(false, true) || this.startPoint.isDone()) {
            throw new IllegalStateException("All steps have already been executed or been canceled");
        }
        this.startPoint.cancel(true);
    }
}
