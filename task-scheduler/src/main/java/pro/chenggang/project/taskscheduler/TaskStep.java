package pro.chenggang.project.taskscheduler;

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
     * The source supplier.
     */
    protected final Supplier<Optional<IN>> source;

    /**
     * The current executor.
     */
    protected final Executor currentExecutor;

    /**
     * Instantiates a new Task step.
     *
     * @param source          the source
     * @param currentExecutor the current executor
     */
    protected TaskStep(Supplier<Optional<IN>> source, Executor currentExecutor) {
        this.source = source;
        this.currentExecutor = currentExecutor;
    }

    /**
     * Source supplier.
     *
     * @return the supplier
     */
    protected abstract Supplier<Optional<OUT>> source();

    /**
     * New task step.
     *
     * @param <T>      the type parameter
     * @param source   the source
     * @param executor the executor
     * @return the task step
     */
    static <T> TaskStep<T, T> newTaskStep(Supplier<Optional<T>> source, Executor executor) {
        return new SourceStep<>(source, executor);
    }

    /**
     * To step execution.
     *
     * @return the step execution
     */
    public StepExecution<OUT, OUT> stepExecution() {
        return StepExecution.newStepExecution(new CompletableFuture<>(), this.source(), this.currentExecutor);
    }

}

/**
 * The Source step.
 *
 * @param <T> the data type
 */
class SourceStep<T> extends TaskStep<T, T> {

    /**
     * Instantiates a new Source step.
     *
     * @param source   the source supplier
     * @param executor the target executor
     */
    protected SourceStep(Supplier<Optional<T>> source, Executor executor) {
        super(source, executor);
    }

    @Override
    protected Supplier<Optional<T>> source() {
        return super.source;
    }
}