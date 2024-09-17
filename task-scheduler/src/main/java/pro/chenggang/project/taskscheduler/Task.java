package pro.chenggang.project.taskscheduler;

import lombok.NonNull;

/**
 * The type Task.
 *
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
public final class Task {

    /**
     * New task task info.
     *
     * @param taskName the task name
     * @return the task info
     */
    public static TaskInfo newTask(@NonNull String taskName){
        return new TaskInfo(taskName);
    }

}
