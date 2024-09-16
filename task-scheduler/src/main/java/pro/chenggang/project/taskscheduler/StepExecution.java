
package pro.chenggang.project.taskscheduler;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The abstract Step execution.
 *
 * @param <IN>  the input data type
 * @param <OUT> the output data type
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
public abstract class StepExecution<IN, OUT> {

    /**
     * New step execution.
     *
     * @param <T>        the type parameter
     * @param startPoint the start point
     * @param source     the source
     * @param executor   the executor
     * @return the step execution
     */
    static <T> StepExecution<T, T> newStepExecution(CompletableFuture<Void> startPoint,
                                                    Supplier<Optional<T>> source,
                                                    Executor executor) {
        return new SourceExecution<>(startPoint, source, executor);
    }

    /**
     * The Start point.
     */
    protected final CompletableFuture<Void> startPoint;
    /**
     * The Source.
     */
    protected final CompletableFuture<Optional<IN>> source;
    /**
     * The Current executor.
     */
    protected final Executor currentExecutor;

    /**
     * Instantiates a new Step execution.
     *
     * @param startPoint the start point
     * @param source     the source
     * @param executor   the executor
     */
    protected StepExecution(CompletableFuture<Void> startPoint,
                            CompletableFuture<Optional<IN>> source,
                            Executor executor) {
        this.startPoint = startPoint;
        this.source = source;
        this.currentExecutor = Objects.nonNull(executor) ? executor : source.defaultExecutor();
    }

    /**
     * Then completable future.
     *
     * @return the completable future
     */
    protected abstract CompletableFuture<Optional<OUT>> then();

    /**
     * Filter step execution.
     *
     * @param predicate the predicate
     * @return the step execution
     */
    public StepExecution<OUT, OUT> filter(Predicate<OUT> predicate) {
        return new FilterExecution<>(this.startPoint, this.then(), this.currentExecutor, predicate);
    }

    /**
     * Validate step execution.
     *
     * @param validator the validator
     * @return the step execution
     */
    public StepExecution<OUT, OUT> validate(Function<OUT, Optional<RuntimeException>> validator) {
        return new ValidatorExecution<>(this.startPoint, this.then(), this.currentExecutor, validator);
    }

    /**
     * Transform step execution.
     *
     * @param <R>               the type parameter
     * @param transformFunction the transform function
     * @return the step execution
     */
    public <R> StepExecution<OUT, R> transform(Function<OUT, Supplier<Optional<R>>> transformFunction) {
        return new TransformExecution<>(this.startPoint, this.then(), this.currentExecutor, transformFunction);
    }

    /**
     * Convert step execution.
     *
     * @param <R>             the type parameter
     * @param convertFunction the convert function
     * @return the step execution
     */
    public <R> StepExecution<OUT, R> convert(Function<OUT, R> convertFunction) {
        return new ConvertExecution<>(this.startPoint, this.then(), this.currentExecutor, convertFunction);
    }

    /**
     * If empty then step execution.
     *
     * @param deferredMono the deferred mono
     * @return the step execution
     */
    public StepExecution<OUT, OUT> ifEmptyThen(Supplier<Optional<OUT>> deferredMono) {
        return new EmptyCheckerExecution<>(this.startPoint, this.then(), this.currentExecutor, deferredMono);
    }

    /**
     * Default when error step execution.
     *
     * @param defaultWhenErrorFunction the default when error function
     * @return the step execution
     */
    public StepExecution<OUT, OUT> defaultWhenError(Function<Throwable, Optional<OUT>> defaultWhenErrorFunction) {
        return new DefaultWhenErrorExecution<>(this.startPoint,
                this.then(),
                this.currentExecutor,
                defaultWhenErrorFunction
        );
    }

    /**
     * To runner step execution runner.
     *
     * @param exceptionHandler the exception handler
     * @return the step execution runner
     */
    public StepExecutionRunner<OUT> toRunner(Consumer<Throwable> exceptionHandler) {
        return new StepExecutionRunner<>(this.startPoint, this.then(), this.currentExecutor, exceptionHandler);
    }

    /**
     * To runner step execution runner.
     *
     * @return the step execution runner
     */
    public StepExecutionRunner<OUT> toRunner() {
        return new StepExecutionRunner<>(this.startPoint, this.then(), this.currentExecutor, null);
    }
}

/**
 * The Source execution.
 *
 * @param <T> the input data type
 */
class SourceExecution<T> extends StepExecution<T, T> {

    /**
     * Instantiates a new Source execution.
     *
     * @param startPoint the start point
     * @param source     the source supplier
     * @param executor   the target executor
     */
    protected SourceExecution(CompletableFuture<Void> startPoint, Supplier<Optional<T>> source, Executor executor) {

        super(startPoint,
                startPoint.thenCompose(aVoid -> {
                    if (Objects.nonNull(executor)) {
                        return CompletableFuture.supplyAsync(source, executor);
                    }
                    return CompletableFuture.supplyAsync(source);
                }),
                executor
        );
    }

    /**
     * Instantiates a new Source execution.
     *
     * @param startPoint the start point
     * @param source     the source future
     * @param executor   the target executor
     */
    protected SourceExecution(CompletableFuture<Void> startPoint,
                              CompletableFuture<Optional<T>> source,
                              Executor executor) {
        super(startPoint, startPoint.thenCompose(aVoid -> source), executor);
    }

    @Override
    public CompletableFuture<Optional<T>> then() {
        return super.source;
    }
}

/**
 * The Filter execution.
 *
 * @param <T> the input data type
 */
class FilterExecution<T> extends StepExecution<T, T> {

    private final Predicate<T> predicate;

    /**
     * Instantiates a new Filter execution.
     *
     * @param startPoint the start point
     * @param source     the source future
     * @param executor   the target executor
     * @param predicate  the predicate
     */
    protected FilterExecution(CompletableFuture<Void> startPoint,
                              CompletableFuture<Optional<T>> source,
                              Executor executor,
                              Predicate<T> predicate) {
        super(startPoint, source, executor);
        this.predicate = predicate;
    }

    @Override
    public CompletableFuture<Optional<T>> then() {
        return super.source.thenApplyAsync(sourceValue -> sourceValue.filter(this.predicate), super.currentExecutor);
    }
}

/**
 * The Validator execution.
 *
 * @param <T> the input data type
 */
class ValidatorExecution<T> extends StepExecution<T, T> {

    private final Function<T, Optional<RuntimeException>> validatorFunction;

    /**
     * Instantiates a new Validator execution.
     *
     * @param startPoint        the start point
     * @param source            the source future
     * @param executor          the target executor
     * @param validatorFunction the validator function
     */
    protected ValidatorExecution(CompletableFuture<Void> startPoint,
                                 CompletableFuture<Optional<T>> source,
                                 Executor executor,
                                 Function<T, Optional<RuntimeException>> validatorFunction) {
        super(startPoint, source, executor);
        this.validatorFunction = validatorFunction;
    }

    @Override
    public CompletableFuture<Optional<T>> then() {
        return super.source
                .thenApplyAsync(sourceValue -> {
                            Optional<RuntimeException> optionalRuntimeException = sourceValue.flatMap(this.validatorFunction);
                            if (optionalRuntimeException.isPresent()) {
                                throw optionalRuntimeException.get();
                            }
                            return sourceValue;
                        },
                        super.currentExecutor
                );
    }
}

/**
 * The Transform execution.
 *
 * @param <T> the input data type
 * @param <R> the other output data type
 */
class TransformExecution<T, R> extends StepExecution<T, R> {

    private final Function<T, Supplier<Optional<R>>> transformFunction;

    /**
     * Instantiates a new Transform execution.
     *
     * @param startPoint        the start point
     * @param source            the source future
     * @param executor          the target executor
     * @param transformFunction the transform function
     */
    protected TransformExecution(CompletableFuture<Void> startPoint,
                                 CompletableFuture<Optional<T>> source,
                                 Executor executor,
                                 Function<T, Supplier<Optional<R>>> transformFunction) {
        super(startPoint, source, executor);
        this.transformFunction = transformFunction;
    }

    @Override
    protected CompletableFuture<Optional<R>> then() {
        return super.source
                .thenComposeAsync(sourceValue -> {
                            if (sourceValue.isEmpty()) {
                                return CompletableFuture.completedFuture(Optional.empty());
                            }
                            return transformFunction
                                    .andThen(supplier -> CompletableFuture.supplyAsync(supplier, super.currentExecutor))
                                    .apply(sourceValue.get());
                        }
                        ,
                        super.currentExecutor
                );
    }
}

/**
 * The Convert execution.
 *
 * @param <T> the input data type
 * @param <R> the output data type
 */
final class ConvertExecution<T, R> extends StepExecution<T, R> {

    private final Function<T, R> convertFunction;

    /**
     * Instantiates a new Convert execution.
     *
     * @param startPoint      the start point
     * @param source          the source future
     * @param executor        the target executor
     * @param convertFunction the convert convertFunction
     */
    protected ConvertExecution(CompletableFuture<Void> startPoint,
                               CompletableFuture<Optional<T>> source,
                               Executor executor,
                               Function<T, R> convertFunction) {
        super(startPoint, source, executor);
        this.convertFunction = convertFunction;
    }

    @Override
    protected CompletableFuture<Optional<R>> then() {
        return super.source
                .thenApplyAsync(sourceValue -> sourceValue.map(convertFunction), super.currentExecutor);
    }
}

/**
 * The Empty checker execution.
 *
 * @param <T> the input data type
 */
class EmptyCheckerExecution<T> extends StepExecution<T, T> {

    private final Supplier<Optional<T>> deferredSupplier;

    /**
     * Instantiates a new Empty checker execution.
     *
     * @param startPoint       the start point
     * @param source           the source future
     * @param executor         the target executor
     * @param deferredSupplier the deferred supplier
     */
    protected EmptyCheckerExecution(CompletableFuture<Void> startPoint,
                                    CompletableFuture<Optional<T>> source,
                                    Executor executor,
                                    Supplier<Optional<T>> deferredSupplier) {
        super(startPoint, source, executor);
        this.deferredSupplier = deferredSupplier;
    }

    @Override
    protected CompletableFuture<Optional<T>> then() {
        return super.source
                .thenComposeAsync(sourceValue -> {
                            if (sourceValue.isPresent()) {
                                return CompletableFuture.completedFuture(sourceValue);
                            }
                            return CompletableFuture.supplyAsync(deferredSupplier, super.currentExecutor);
                        }
                        ,
                        super.currentExecutor
                );
    }
}


/**
 * The type Default when error execution.
 *
 * @param <T> the type parameter
 */
class DefaultWhenErrorExecution<T> extends StepExecution<T, T> {

    private final Function<Throwable, Optional<T>> defaultWhenErrorFunction;


    /**
     * Instantiates a new Default when error execution.
     *
     * @param startPoint               the start point
     * @param source                   the source
     * @param executor                 the executor
     * @param defaultWhenErrorFunction the default when error function
     */
    protected DefaultWhenErrorExecution(CompletableFuture<Void> startPoint,
                                        CompletableFuture<Optional<T>> source,
                                        Executor executor,
                                        Function<Throwable, Optional<T>> defaultWhenErrorFunction) {
        super(startPoint, source, executor);
        this.defaultWhenErrorFunction = defaultWhenErrorFunction;
    }

    @Override
    protected CompletableFuture<Optional<T>> then() {
        return source.exceptionallyAsync(throwable -> {
                    if (throwable instanceof CompletionException completionException) {
                        throwable = completionException.getCause();
                    }
                    return defaultWhenErrorFunction.apply(throwable);
                },
                super.currentExecutor
        );
    }
}