package pro.chenggang.project.taskscheduler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
@Order(2)
public class TaskStepTests {

    String text = "Tests";

    TaskInfo taskInfo = new TaskInfo("TaskStepTests");

    @Test
    void testSourceStep() {
        SourceStep<String> sourceStep = new SourceStep<>(taskInfo, "testSourceStep", () -> Optional.of(text), null);
        Optional<String> optionalResult = sourceStep.runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testNewTaskStep() {
        TaskStep<String, String> taskStep = TaskStep.newTaskStep(taskInfo,
                "testNewTaskStep",
                () -> Optional.of(text),
                null
        );
        Optional<String> optionalResult = taskStep.runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testToStepExecution() {
        StepExecution<String, String> stepExecution = TaskStep.newTaskStep(taskInfo,
                        "testToStepExecution",
                        () -> Optional.of(text),
                        null
                )
                .stepExecution();
        stepExecution.startPoint.complete(null);
        Optional<String> optionalResult = stepExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testNewNextTaskStep() {
        StepExecution<String, String> stepExecution = TaskStep.newNextStep(taskInfo, "next - testNewNextTaskStep",
                        new StepExecutionRunner<>(TaskStepInfo.of("TaskStepTests", "testNewNextTaskStep"),
                                new CompletableFuture<>(),
                                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                                null,
                                null
                        ),
                        null
                )
                .stepExecution();
        stepExecution.startPoint.complete(null);
        Optional<String> optionalResult = stepExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testRunAll() {
        Optional<String> optionalResult = TaskStep.newNextStep(taskInfo, "next - testNewNextTaskStep",
                        new StepExecutionRunner<>(TaskStepInfo.of("TaskStepTests", "testNewNextTaskStep"),
                                new CompletableFuture<>(),
                                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                                null,
                                null
                        ),
                        null
                )
                .stepExecution()
                .endTaskStep()
                .runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
        taskInfo.getTaskStepInfos().forEach(taskStepInfo -> System.out.println(taskStepInfo.summary()));
    }

    @Test
    void testCancelAll() throws Exception {
        TaskInfo resultTaskInfo = TaskStep.newTaskStep(taskInfo, "next - testNewNextTaskStep",
                        () -> Optional.of(text),
                        null
                )
                .stepExecution()
                .endTaskStep()
                .cancelAllSteps();
        TimeUnit.SECONDS.sleep(5);
        resultTaskInfo.getTaskStepInfos().forEach(taskStepInfo -> System.out.println(taskStepInfo.summary()));
    }
}
