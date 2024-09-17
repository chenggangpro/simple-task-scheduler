
package pro.chenggang.project.taskscheduler;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static pro.chenggang.project.taskscheduler.TaskStep.executorToString;

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
     * @param <T>          the type parameter
     * @param taskInfo     the task info
     * @param taskStepInfo the task step info
     * @param startPoint   the start point
     * @param sourceFuture the source future
     * @param executor     the executor
     * @return the step execution
     */
    static <T> StepExecution<T, T> newStepExecution(@NonNull TaskInfo taskInfo,
                                                    @NonNull TaskStepInfo taskStepInfo,
                                                    @NonNull CompletableFuture<Void> startPoint,
                                                    @NonNull CompletableFuture<Optional<T>> sourceFuture,
                                                    Executor executor) {
        return new SourceExecution<>(taskInfo, taskStepInfo, startPoint, sourceFuture, executor);
    }

    /**
     * The Task info.
     */
    protected final TaskInfo taskInfo;

    /**
     * The Task step info.
     */
    protected final TaskStepInfo taskStepInfo;

    /**
     * The Start point.
     */
    protected final CompletableFuture<Void> startPoint;

    /**
     * The source future.
     */
    protected final CompletableFuture<Optional<IN>> sourceFuture;

    /**
     * The Current executor.
     */
    protected final Executor currentExecutor;

    /**
     * Instantiates a new Step execution.
     *
     * @param taskInfo     the task info
     * @param taskStepInfo the task step info
     * @param startPoint   the start point
     * @param sourceFuture the source future
     * @param executor     the executor
     */
    protected StepExecution(@NonNull TaskInfo taskInfo,
                            @NonNull TaskStepInfo taskStepInfo,
                            @NonNull CompletableFuture<Void> startPoint,
                            @NonNull CompletableFuture<Optional<IN>> sourceFuture,
                            Executor executor) {
        this.taskInfo = taskInfo;
        this.taskStepInfo = taskStepInfo;
        this.startPoint = startPoint;
        this.sourceFuture = sourceFuture;
        this.currentExecutor = Objects.nonNull(executor) ? executor : sourceFuture.defaultExecutor();
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
    public StepExecution<OUT, OUT> filter(@NonNull Predicate<OUT> predicate) {
        return new FilterExecution<>(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.then(),
                this.currentExecutor,
                predicate
        );
    }

    /**
     * Validate step execution.
     *
     * @param validator the validator
     * @return the step execution
     */
    public StepExecution<OUT, OUT> validate(@NonNull Function<OUT, Optional<RuntimeException>> validator) {
        return new ValidatorExecution<>(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.then(),
                this.currentExecutor,
                validator
        );
    }

    /**
     * Transform step execution.
     *
     * @param <R>               the type parameter
     * @param transformFunction the transform function
     * @return the step execution
     */
    public <R> StepExecution<OUT, R> transform(@NonNull Function<OUT, Supplier<Optional<R>>> transformFunction) {
        return new TransformExecution<>(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.then(),
                this.currentExecutor,
                transformFunction
        );
    }

    /**
     * Convert step execution.
     *
     * @param <R>             the type parameter
     * @param convertFunction the convert function
     * @return the step execution
     */
    public <R> StepExecution<OUT, R> convert(@NonNull Function<OUT, R> convertFunction) {
        return new ConvertExecution<>(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.then(),
                this.currentExecutor,
                convertFunction
        );
    }

    /**
     * If empty then step execution.
     *
     * @param deferredMono the deferred mono
     * @return the step execution
     */
    public StepExecution<OUT, OUT> ifEmptyThen(@NonNull Supplier<Optional<OUT>> deferredMono) {
        return new EmptyCheckerExecution<>(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.then(),
                this.currentExecutor,
                deferredMono
        );
    }

    /**
     * Default when error step execution.
     *
     * @param defaultWhenErrorFunction the default when error function
     * @return the step execution
     */
    public StepExecution<OUT, OUT> defaultWhenError(@NonNull Function<Throwable, Optional<OUT>> defaultWhenErrorFunction) {
        return new DefaultWhenErrorExecution<>(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.then(),
                this.currentExecutor,
                defaultWhenErrorFunction
        );
    }


    /**
     * End task step.
     *
     * @return the task step
     */
    public TaskStep<OUT, OUT> endTaskStep() {
        StepExecutionRunner<OUT> stepExecutionRunner = new StepExecutionRunner<>(this.taskStepInfo, this.startPoint,
                this.then(),
                this.currentExecutor,
                null
        );
        return TaskStep.newNextStep(taskInfo,
                "END::" + taskStepInfo.getStepName(),
                stepExecutionRunner,
                this.currentExecutor
        );
    }

    /**
     * Exceptionally then end task step.
     *
     * @param exceptionHandler the exception handler
     * @return the task step
     */
    public TaskStep<OUT, OUT> exceptionallyThenEndTaskStep(@NonNull Consumer<Throwable> exceptionHandler) {
        StepExecutionRunner<OUT> stepExecutionRunner = new StepExecutionRunner<>(this.taskStepInfo, this.startPoint,
                this.then(),
                this.currentExecutor,
                exceptionHandler
        );
        return TaskStep.newNextStep(taskInfo,
                "END::" + taskStepInfo.getStepName(),
                stepExecutionRunner,
                this.currentExecutor
        );
    }

    /**
     * Next task step.
     *
     * @param nextTaskStepName the next task step name
     * @return the task step
     */
    public TaskStep<OUT, OUT> nextTaskStep(@NonNull String nextTaskStepName) {
        StepExecutionRunner<OUT> stepExecutionRunner = new StepExecutionRunner<>(this.taskStepInfo, this.startPoint,
                this.then(),
                this.currentExecutor,
                null
        );
        return TaskStep.newNextStep(taskInfo, nextTaskStepName, stepExecutionRunner, this.currentExecutor);
    }

    /**
     * Exceptionally then next task step.
     *
     * @param nextTaskStepName the next task step name
     * @param exceptionHandler the exception handler
     * @return the task step
     */
    public TaskStep<OUT, OUT> exceptionallyThenNextTaskStep(@NonNull String nextTaskStepName,
                                                            @NonNull Consumer<Throwable> exceptionHandler) {
        StepExecutionRunner<OUT> stepExecutionRunner = new StepExecutionRunner<>(this.taskStepInfo, this.startPoint,
                this.then(),
                this.currentExecutor,
                exceptionHandler
        );
        return TaskStep.newNextStep(taskInfo, nextTaskStepName, stepExecutionRunner, this.currentExecutor);
    }

    /**
     * Next task step.
     *
     * @param nextTaskStepName the next task step name
     * @param nextExecutor     the next executor
     * @return the task step
     */
    public TaskStep<OUT, OUT> nextTaskStep(@NonNull String nextTaskStepName, @NonNull Executor nextExecutor) {
        StepExecutionRunner<OUT> stepExecutionRunner = new StepExecutionRunner<>(this.taskStepInfo, this.startPoint,
                this.then(),
                this.currentExecutor,
                null
        );
        return TaskStep.newNextStep(taskInfo, nextTaskStepName, stepExecutionRunner, nextExecutor);
    }

    /**
     * Exceptionally then next task step.
     *
     * @param nextTaskStepName the next task step name
     * @param exceptionHandler the exception handler
     * @param nextExecutor     the next executor
     * @return the task step
     */
    public TaskStep<OUT, OUT> exceptionallyThenNextTaskStep(@NonNull String nextTaskStepName,
                                                            @NonNull Consumer<Throwable> exceptionHandler,
                                                            @NonNull Executor nextExecutor) {
        StepExecutionRunner<OUT> stepExecutionRunner = new StepExecutionRunner<>(this.taskStepInfo, this.startPoint,
                this.then(),
                this.currentExecutor,
                exceptionHandler
        );
        return TaskStep.newNextStep(taskInfo, nextTaskStepName, stepExecutionRunner, nextExecutor);
    }
}

/**
 * The Source execution.
 *
 * @param <T> the input data type
 */
@Slf4j
class SourceExecution<T> extends StepExecution<T, T> {

    /**
     * Instantiates a new Source execution.
     *
     * @param taskInfo     the task info
     * @param taskStepInfo the task step info
     * @param startPoint   the start point
     * @param source       the source future
     * @param executor     the target executor
     */
    protected SourceExecution(TaskInfo taskInfo,
                              TaskStepInfo taskStepInfo,
                              CompletableFuture<Void> startPoint,
                              CompletableFuture<Optional<T>> source,
                              Executor executor) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
    }

    @Override
    public CompletableFuture<Optional<T>> then() {
        log.debug("StepExecution::Assemble::SourceExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
        return super.sourceFuture;
    }
}

/**
 * The Filter execution.
 *
 * @param <T> the input data type
 */
@Slf4j
class FilterExecution<T> extends StepExecution<T, T> {

    private final Predicate<T> predicate;

    /**
     * Instantiates a new Filter execution.
     *
     * @param taskInfo     the task info
     * @param taskStepInfo the task step info
     * @param startPoint   the start point
     * @param source       the source future
     * @param executor     the target executor
     * @param predicate    the predicate
     */
    protected FilterExecution(TaskInfo taskInfo,
                              TaskStepInfo taskStepInfo,
                              CompletableFuture<Void> startPoint,
                              CompletableFuture<Optional<T>> source,
                              Executor executor,
                              Predicate<T> predicate) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
        this.predicate = predicate;
    }

    @Override
    public CompletableFuture<Optional<T>> then() {
        log.debug("StepExecution::Assemble::FilterExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
        return super.sourceFuture.thenApplyAsync(sourceValue -> {
                    log.debug("StepExecution::Running::FilterExecution => Task:{},Executor:{}",
                            taskStepInfo,
                            executorToString(this.currentExecutor)
                    );
                    return sourceValue.filter(this.predicate);
                }, super.currentExecutor
        );
    }
}

/**
 * The Validator execution.
 *
 * @param <T> the input data type
 */
@Slf4j
class ValidatorExecution<T> extends StepExecution<T, T> {

    private final Function<T, Optional<RuntimeException>> validatorFunction;

    /**
     * Instantiates a new Validator execution.
     *
     * @param taskInfo          the task info
     * @param taskStepInfo      the task step info
     * @param startPoint        the start point
     * @param source            the source future
     * @param executor          the target executor
     * @param validatorFunction the validator function
     */
    protected ValidatorExecution(TaskInfo taskInfo,
                                 TaskStepInfo taskStepInfo,
                                 CompletableFuture<Void> startPoint,
                                 CompletableFuture<Optional<T>> source,
                                 Executor executor,
                                 Function<T, Optional<RuntimeException>> validatorFunction) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
        this.validatorFunction = validatorFunction;
    }

    @Override
    public CompletableFuture<Optional<T>> then() {
        log.debug("StepExecution::Assemble::ValidatorExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
        return super.sourceFuture
                .thenApplyAsync(sourceValue -> {
                            log.debug("StepExecution::Running::ValidatorExecution => Task:{},Executor:{}",
                                    taskStepInfo.getFullName(),
                                    currentExecutor
                            );
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
@Slf4j
class TransformExecution<T, R> extends StepExecution<T, R> {

    private final Function<T, Supplier<Optional<R>>> transformFunction;

    /**
     * Instantiates a new Transform execution.
     *
     * @param taskInfo          the task info
     * @param taskStepInfo      the task step info
     * @param startPoint        the start point
     * @param source            the source future
     * @param executor          the target executor
     * @param transformFunction the transform function
     */
    protected TransformExecution(TaskInfo taskInfo,
                                 TaskStepInfo taskStepInfo,
                                 CompletableFuture<Void> startPoint,
                                 CompletableFuture<Optional<T>> source,
                                 Executor executor,
                                 Function<T, Supplier<Optional<R>>> transformFunction) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
        this.transformFunction = transformFunction;
    }

    @Override
    protected CompletableFuture<Optional<R>> then() {
        log.debug("StepExecution::Assemble::TransformExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
        return super.sourceFuture
                .thenComposeAsync(sourceValue -> {
                            log.debug("StepExecution::Running::TransformExecution => Task:{},Executor:{}",
                                    taskStepInfo.getFullName(),
                                    currentExecutor
                            );
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
@Slf4j
class ConvertExecution<T, R> extends StepExecution<T, R> {

    private final Function<T, R> convertFunction;

    /**
     * Instantiates a new Convert execution.
     *
     * @param taskInfo        the task info
     * @param taskStepInfo    the task step info
     * @param startPoint      the start point
     * @param source          the source future
     * @param executor        the target executor
     * @param convertFunction the convert convertFunction
     */
    protected ConvertExecution(TaskInfo taskInfo,
                               TaskStepInfo taskStepInfo,
                               CompletableFuture<Void> startPoint,
                               CompletableFuture<Optional<T>> source,
                               Executor executor,
                               Function<T, R> convertFunction) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
        this.convertFunction = convertFunction;
    }

    @Override
    protected CompletableFuture<Optional<R>> then() {
        log.debug("StepExecution::Assemble::ConvertExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
        return super.sourceFuture
                .thenApplyAsync(sourceValue -> {
                            log.debug("StepExecution::Running::ConvertExecution => Task:{},Executor:{}",
                                    taskStepInfo.getFullName(),
                                    currentExecutor
                            );
                            return sourceValue.map(convertFunction);
                        }, super.currentExecutor
                );
    }
}

/**
 * The Empty checker execution.
 *
 * @param <T> the input data type
 */
@Slf4j
class EmptyCheckerExecution<T> extends StepExecution<T, T> {

    private final Supplier<Optional<T>> deferredSupplier;

    /**
     * Instantiates a new Empty checker execution.
     *
     * @param taskInfo         the task info
     * @param taskStepInfo     the task step info
     * @param startPoint       the start point
     * @param source           the source future
     * @param executor         the target executor
     * @param deferredSupplier the deferred supplier
     */
    protected EmptyCheckerExecution(TaskInfo taskInfo,
                                    TaskStepInfo taskStepInfo,
                                    CompletableFuture<Void> startPoint,
                                    CompletableFuture<Optional<T>> source,
                                    Executor executor,
                                    Supplier<Optional<T>> deferredSupplier) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
        this.deferredSupplier = deferredSupplier;
    }

    @Override
    protected CompletableFuture<Optional<T>> then() {
        log.debug("StepExecution::Assemble::EmptyCheckerExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
        return super.sourceFuture
                .thenComposeAsync(sourceValue -> {
                            log.debug("StepExecution::Running::EmptyCheckerExecution => Task:{},Executor:{}",
                                    taskStepInfo.getFullName(),
                                    currentExecutor
                            );
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
@Slf4j
class DefaultWhenErrorExecution<T> extends StepExecution<T, T> {

    private final Function<Throwable, Optional<T>> defaultWhenErrorFunction;


    /**
     * Instantiates a new Default when error execution.
     *
     * @param taskInfo                 the task info
     * @param taskStepInfo             the task step info
     * @param startPoint               the start point
     * @param source                   the source
     * @param executor                 the executor
     * @param defaultWhenErrorFunction the default when error function
     */
    protected DefaultWhenErrorExecution(TaskInfo taskInfo,
                                        TaskStepInfo taskStepInfo,
                                        CompletableFuture<Void> startPoint,
                                        CompletableFuture<Optional<T>> source,
                                        Executor executor,
                                        Function<Throwable, Optional<T>> defaultWhenErrorFunction) {
        super(taskInfo, taskStepInfo, startPoint, source, executor);
        this.defaultWhenErrorFunction = defaultWhenErrorFunction;
    }

    @Override
    protected CompletableFuture<Optional<T>> then() {
        log.debug("StepExecution::Assemble::DefaultWhenErrorExecution => Task:{},Executor:{}",
                taskStepInfo.getFullName(),
                currentExecutor
        );
        return sourceFuture.exceptionallyAsync(throwable -> {
                    log.debug("StepExecution::Running::DefaultWhenErrorExecution => Task:{},Executor:{}",
                            taskStepInfo.getFullName(),
                            currentExecutor
                    );
                    if (throwable instanceof CompletionException completionException) {
                        throwable = completionException.getCause();
                    }
                    return defaultWhenErrorFunction.apply(throwable);
                },
                super.currentExecutor
        );
    }
}