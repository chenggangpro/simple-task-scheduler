package pro.chenggang.project.taskscheduler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

/**
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
@Order(0)
public class StepExecutionTests {

    String text = "Tests";

    TaskInfo taskInfo = new TaskInfo("StepExecutionTests");

    CompletableFuture<Void> startPoint;

    @BeforeEach
    void beforeEach() {
        startPoint = new CompletableFuture<>();
    }


    @Test
    void testSourceExecutionWithCompletableFuture() {
        SourceExecution<String> sourceExecution = new SourceExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testSourceExecutionWithCompletableFuture"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        Optional<String> optionalResult = sourceExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testConverterExecution() {
        ConvertExecution<String, String> sourceExecution = new ConvertExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testConverterExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor(),
                value -> text.toUpperCase()
        );
        startPoint.complete(null);
        Optional<String> optionalResult = sourceExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text.toUpperCase());
    }

    @Test
    void testDefaultWhenErrorExecution() {
        DefaultWhenErrorExecution<String> defaultWhenErrorExecution = new DefaultWhenErrorExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testDefaultWhenErrorExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> {
                    Optional<String> optionalValue = Optional.of(text);
                    if (optionalValue.isPresent()) {
                        throw new RuntimeException("Fake Error");
                    }
                    return optionalValue;
                }),
                Executors.newSingleThreadExecutor(),
                throwable -> {
                    String message = throwable.getMessage();
                    if ("Fake Error".equals(message)) {
                        return Optional.of("Default");
                    }
                    return Optional.empty();
                }
        );
        startPoint.complete(null);
        Optional<String> optionalResult = defaultWhenErrorExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), "Default");
    }

    @Test
    void testEmptyCheckerExecution() {
        EmptyCheckerExecution<String> emptyCheckerExecution = new EmptyCheckerExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testEmptyCheckerExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.empty()),
                Executors.newSingleThreadExecutor(),
                () -> Optional.of("Empty")
        );
        startPoint.complete(null);
        Optional<String> optionalResult = emptyCheckerExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), "Empty");
    }

    @Test
    void testEmptyCheckerExecutionWithValue() {
        EmptyCheckerExecution<String> emptyCheckerExecution = new EmptyCheckerExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testEmptyCheckerExecutionWithValue"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor(),
                () -> Optional.of("Empty")
        );
        startPoint.complete(null);
        Optional<String> optionalResult = emptyCheckerExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testFilterExecution() {
        FilterExecution<String> filterExecution = new FilterExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testFilterExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor(),
                value -> !text.equals(value)
        );
        startPoint.complete(null);
        Optional<String> optionalResult = filterExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isEmpty());
    }

    @Test
    void testTransformExecution() {
        TransformExecution<String, String> transformExecution = new TransformExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testTransformExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor(),
                value -> () -> Optional.of(value.toUpperCase())
        );
        startPoint.complete(null);
        Optional<String> optionalResult = transformExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text.toUpperCase());
    }

    @Test
    void testTransformExecutionWithEmptySource() {
        TransformExecution<String, String> transformExecution = new TransformExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testTransformExecutionWithEmptySource"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.empty()),
                Executors.newSingleThreadExecutor(),
                value -> () -> Optional.of(value.toUpperCase())
        );
        startPoint.complete(null);
        Optional<String> optionalResult = transformExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isEmpty());
    }

    @Test
    void testValidatorExecution() {
        ValidatorExecution<String> validatorExecution = new ValidatorExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testValidatorExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor(),
                value -> {
                    if (text.equals(value)) {
                        return Optional.of(new RuntimeException("Validator"));
                    }
                    return Optional.empty();
                }
        );
        startPoint.complete(null);
        Optional<String> optionalResult = null;
        try {
            optionalResult = validatorExecution.then()
                    .join();
        } catch (CompletionException e) {
            Assertions.assertNotNull(e);
            Assertions.assertInstanceOf(e.getCause().getClass(), new RuntimeException());
            Assertions.assertEquals(e.getCause().getMessage(), "Validator");
        }
        Assertions.assertNull(optionalResult);
    }

    @Test
    void testValidatorExecutionWithEmptySource() {
        ValidatorExecution<String> validatorExecution = new ValidatorExecution<>(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testValidatorExecutionWithEmptySource"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.empty()),
                Executors.newSingleThreadExecutor(),
                value -> {
                    if (text.equals(value)) {
                        return Optional.of(new RuntimeException("Validator"));
                    }
                    return Optional.empty();
                }
        );
        startPoint.complete(null);
        Optional<String> optionalResult = validatorExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isEmpty());
    }

    @Test
    void testNewStepExecution() {
        StepExecution<String, String> stepExecution = StepExecution.newStepExecution(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testNewStepExecution"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        startPoint.complete(null);
        Optional<String> optionalResult = stepExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testNextTaskStep() {
        StepExecution<String, String> stepExecution = StepExecution.newStepExecution(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "firstTaskStep"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        TaskStep<String, String> nextTaskStep = stepExecution.nextTaskStep("nextTaskStep");
        Optional<String> optionalResult = nextTaskStep.runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testExceptionallyThenNextTaskStep() {
        StepExecution<String, String> stepExecution = StepExecution.newStepExecution(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testExceptionallyThenNextTaskStep"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        TaskStep<String, String> nextTaskStep = stepExecution.exceptionallyThenNextTaskStep("nextTaskStep",
                throwable -> System.out.println(throwable.getMessage())
        );
        Optional<String> optionalResult = nextTaskStep.runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testNextTaskStepWithExecutor() {
        StepExecution<String, String> stepExecution = StepExecution.newStepExecution(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testNextTaskStepWithExecutor"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        TaskStep<String, String> nextTaskStep = stepExecution.nextTaskStep("nextTaskStep",
                Executors.newSingleThreadExecutor(r -> new Thread(r, "another"))
        );
        Optional<String> optionalResult = nextTaskStep.runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testExceptionallyThenNextTaskStepWithExecutor() {
        StepExecution<String, String> stepExecution = StepExecution.newStepExecution(taskInfo,
                TaskStepInfo.of(taskInfo.getTaskName(), "testExceptionallyThenNextTaskStepWithExecutor"),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        TaskStep<String, String> nextTaskStep = stepExecution.exceptionallyThenNextTaskStep("nextTaskStep",
                throwable -> System.out.println(throwable.getMessage()),
                Executors.newSingleThreadExecutor(r -> new Thread(r, "another"))
        );
        Optional<String> optionalResult = nextTaskStep.runAllSteps();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

}
