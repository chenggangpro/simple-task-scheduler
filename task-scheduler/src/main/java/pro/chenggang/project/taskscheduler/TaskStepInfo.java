package pro.chenggang.project.taskscheduler;

import lombok.NonNull;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * The type Task step info.
 *
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
public final class TaskStepInfo {

    private final String taskName;

    private final String stepName;

    private final Instant creationTime;

    private Instant startTime;

    private Instant endTime;

    private boolean isCanceled;

    private TaskStepInfo(@NonNull String taskName, @NonNull String stepName) {
        this.taskName = taskName;
        this.stepName = stepName;
        this.creationTime = Instant.now();
    }

    /**
     * New task step info.
     *
     * @param taskName the task name
     * @param stepName the step name
     * @return the task step info
     */
    static TaskStepInfo of(@NonNull String taskName, @NonNull String stepName) {
        return new TaskStepInfo(taskName, stepName);
    }

    /**
     * Record start time of task info.
     */
    void start() {
        this.startTime = Instant.now();
        this.endTime = null;
    }

    /**
     * Record end time of task info.
     */
    void stop() {
        if (Objects.isNull(this.startTime)) {
            throw new IllegalArgumentException("The task step : (" + this.stepName + ") didn't started");
        }
        this.endTime = Instant.now();
    }

    /**
     * Canceled.
     */
    void canceled() {
        this.isCanceled = true;
        this.endTime = Instant.now();
    }

    /**
     * Gets step name.
     *
     * @return the step name
     */
    public String getStepName() {
        return stepName;
    }

    /**
     * Is canceled boolean.
     *
     * @return the boolean
     */
    public boolean isCanceled() {
        return isCanceled;
    }

    /**
     * Gets full name.
     *
     * @return the step name
     */
    public String getFullName() {
        return this.taskName + "::" + this.stepName;
    }

    /**
     * Cost duration.
     *
     * @return the duration
     */
    public Duration cost() {
        if (Objects.isNull(startTime) || Objects.isNull(endTime)) {
            return Duration.ZERO;
        }
        return Duration.between(startTime, endTime);
    }

    /**
     * Gets a summary string.
     *
     * @return the string
     */
    public String summary() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String summaryFormat = "{\"Task Step\":\"%s\", \"Canceled\":\"%s\", \"Creation Time\": \"%s\", \"Start Time\": \"%s\", \"End Time\": \"%s\", \"Cost Duration\": \"%s\"}";
        return String.format(summaryFormat,
                this.stepName,
                this.isCanceled,
                formatter.format(creationTime.atZone(ZoneId.systemDefault())),
                Objects.isNull(startTime) ? "null" : formatter.format(startTime.atZone(ZoneId.systemDefault())),
                Objects.isNull(endTime) ? "null" : formatter.format(endTime.atZone(ZoneId.systemDefault())),
                (Objects.isNull(startTime) || Objects.isNull(endTime)) ? "null" : Duration.between(startTime, endTime)
        );
    }

}
