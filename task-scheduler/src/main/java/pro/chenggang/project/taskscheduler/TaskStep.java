package pro.chenggang.project.taskscheduler;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * The Task step.
 *
 * @param <IN>  the input data type
 * @param <OUT> the output data type
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
public abstract class TaskStep<IN, OUT> {

    /**
     * Executor to string.
     *
     * @param executor the executor
     * @return the string
     */
    static String executorToString(Executor executor) {
        if (Objects.isNull(executor)) {
            return null;
        }
        return executor.getClass().getSimpleName() + "@" + Integer.toHexString(executor.hashCode());
    }

    /**
     * The task info.
     */
    protected final TaskInfo taskInfo;

    /**
     * The task step info
     */
    protected final TaskStepInfo taskStepInfo;

    /**
     * The start point
     */
    protected final CompletableFuture<Void> startPoint;

    /**
     * The source future.
     */
    protected final CompletableFuture<Optional<IN>> sourceFuture;

    /**
     * The current executor.
     */
    protected final Executor currentExecutor;


    /**
     * Instantiates a new Task step.
     *
     * @param taskInfo        the task info
     * @param stepName        the step name
     * @param startPoint      the start point
     * @param sourceFuture    the source future
     * @param currentExecutor the current executor
     */
    protected TaskStep(@NonNull TaskInfo taskInfo,
                       @NonNull String stepName,
                       @NonNull CompletableFuture<Void> startPoint,
                       @NonNull CompletableFuture<Optional<IN>> sourceFuture,
                       Executor currentExecutor) {
        this.taskInfo = taskInfo;
        this.startPoint = startPoint;
        this.sourceFuture = sourceFuture;
        this.currentExecutor = currentExecutor;
        this.taskStepInfo = this.taskInfo.addStep(stepName);
    }

    protected TaskStep(@NonNull TaskInfo taskInfo,
                       @NonNull String stepName,
                       @NonNull CompletableFuture<Void> startPoint,
                       @NonNull Supplier<Optional<IN>> sourceSupplier,
                       Executor currentExecutor) {
        this.taskInfo = taskInfo;
        this.startPoint = startPoint;
        this.sourceFuture = this.startPoint.thenCompose(aVoid ->
                Objects.isNull(currentExecutor) ?
                        CompletableFuture.supplyAsync(sourceSupplier) : CompletableFuture.supplyAsync(sourceSupplier,
                        currentExecutor
                ));
        this.currentExecutor = currentExecutor;
        this.taskStepInfo = this.taskInfo.addStep(stepName);
    }

    /**
     * Over all completable future.
     *
     * @return the completable future
     */
    protected abstract CompletableFuture<Optional<OUT>> overAll();

    /**
     * To step execution.
     *
     * @return the step execution
     */
    public abstract StepExecution<OUT, OUT> stepExecution();

    /**
     * Gets task info.
     *
     * @return the task info
     */
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    /**
     * Run all steps.
     *
     * @return the optional result
     */
    public Optional<OUT> runAllSteps() {
        this.startPoint.complete(null);
        return this.overAll().join();
    }

    /**
     * Cancel all steps task info.
     *
     * @return the task info
     */
    public TaskInfo cancelAllSteps() {
        this.startPoint.cancel(true);
        this.taskInfo.getTaskStepInfos().forEach(TaskStepInfo::canceled);
        return this.taskInfo;
    }

    /**
     * New task step.
     *
     * @param <T>      the type parameter
     * @param taskInfo the task info
     * @param stepName the step name
     * @param source   the source
     * @param executor the executor
     * @return the task step
     */
    static <T> TaskStep<T, T> newTaskStep(TaskInfo taskInfo,
                                          String stepName,
                                          Supplier<Optional<T>> source,
                                          Executor executor) {
        return new SourceStep<>(taskInfo, stepName, source, executor);
    }

    /**
     * New next task step.
     *
     * @param <T>                       the type parameter
     * @param taskInfo                  the task info
     * @param stepName                  the step name
     * @param sourceStepExecutionRunner the source step execution runner
     * @param executor                  the executor
     * @return the task step
     */
    static <T> TaskStep<T, T> newNextStep(TaskInfo taskInfo,
                                          String stepName,
                                          StepExecutionRunner<T> sourceStepExecutionRunner,
                                          Executor executor) {
        return new NextStep<>(taskInfo, stepName, sourceStepExecutionRunner, executor);
    }

}

/**
 * The Source step.
 *
 * @param <T> the data type
 */
@Slf4j
class SourceStep<T> extends TaskStep<T, T> {

    /**
     * Instantiates a new Source step.
     *
     * @param taskInfo the task info
     * @param stepName the stepName
     * @param source   the source supplier
     * @param executor the target executor
     */
    protected SourceStep(TaskInfo taskInfo, String stepName, Supplier<Optional<T>> source, Executor executor) {
        super(taskInfo,
                stepName,
                new CompletableFuture<>(),
                source,
                executor
        );

        log.debug("TaskStep::Assemble::SourceStep => Name:{},Executor:{}",
                this.taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
    }

    @Override
    protected CompletableFuture<Optional<T>> overAll() {
        return this.sourceFuture;
    }

    @Override
    public StepExecution<T, T> stepExecution() {
        return StepExecution.newStepExecution(this.taskInfo,
                this.taskStepInfo,
                this.startPoint,
                this.overAll(),
                this.currentExecutor
        );
    }
}

/**
 * The type next step.
 *
 * @param <T> the type parameter
 */
@Slf4j
class NextStep<T> extends TaskStep<T, T> {

    /**
     * The Source step execution runner.
     */
    protected final StepExecutionRunner<T> sourceStepExecutionRunner;

    /**
     * Instantiates a new next step.
     *
     * @param taskInfo                  the task info
     * @param stepName                  the stepName
     * @param sourceStepExecutionRunner the source step execution runner
     * @param executor                  the executor
     */
    protected NextStep(TaskInfo taskInfo,
                       String stepName,
                       StepExecutionRunner<T> sourceStepExecutionRunner,
                       Executor executor) {
        super(taskInfo,
                stepName,
                sourceStepExecutionRunner.startPoint(),
                sourceStepExecutionRunner.overAll(),
                executor
        );
        this.sourceStepExecutionRunner = sourceStepExecutionRunner;
        log.debug("TaskStep::Assemble::NextStep => Name:{},Executor:{}",
                this.taskStepInfo.getFullName(),
                executorToString(this.currentExecutor)
        );
    }

    @Override
    protected CompletableFuture<Optional<T>> overAll() {
        return this.sourceFuture;
    }

    @Override
    public StepExecution<T, T> stepExecution() {
        return StepExecution.newStepExecution(this.taskInfo,
                this.taskStepInfo,
                this.sourceStepExecutionRunner.startPoint(),
                this.sourceFuture,
                this.currentExecutor
        );
    }
}