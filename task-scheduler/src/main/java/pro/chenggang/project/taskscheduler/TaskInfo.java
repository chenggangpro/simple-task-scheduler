package pro.chenggang.project.taskscheduler;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * The type Task info.
 *
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
public final class TaskInfo {

    private final String taskName;

    private final List<TaskStepInfo> taskSteps = new ArrayList<>();

    /**
     * Instantiates a new Task info.
     *
     * @param taskName the task name
     */
    TaskInfo(String taskName) {
        this.taskName = taskName;
    }

    /**
     * Gets task name.
     *
     * @return the task name
     */
    public String getTaskName() {
        return taskName;
    }

    /**
     * New task step.
     *
     * @param <T>      the type parameter
     * @param stepName the step name
     * @param source   the source
     * @param executor the executor
     * @return the task step
     */
    public <T> TaskStep<T, T> newTaskStep(@NonNull String stepName,
                                          @NonNull Supplier<Optional<T>> source,
                                          Executor executor) {
        return TaskStep.newTaskStep(this, stepName, source, executor);
    }

    /**
     * Add step.
     *
     * @param stepName the step name
     * @return the task step info
     */
    TaskStepInfo addStep(@NonNull String stepName) {
        TaskStepInfo taskStepInfo = TaskStepInfo.of(this.taskName, stepName);
        this.taskSteps.add(taskStepInfo);
        return taskStepInfo;
    }

    /**
     * Gets task step infos.
     *
     * @return the task step infos
     */
    public List<TaskStepInfo> getTaskStepInfos() {
        return List.copyOf(this.taskSteps);
    }
}
