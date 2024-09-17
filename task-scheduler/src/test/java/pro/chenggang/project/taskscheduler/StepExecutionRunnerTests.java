package pro.chenggang.project.taskscheduler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/**
 * @author Gang Cheng
 * @version 1.0.0
 * @since 1.0.0
 */
@Order(1)
public class StepExecutionRunnerTests {

    String text = "Tests";

    CompletableFuture<Void> startPoint;

    @BeforeEach
    void beforeEach() {
        startPoint = new CompletableFuture<>();
    }

    @Test
    void testRunWithNoException() {
        StepExecutionRunner<String> stepExecutionRunner = new StepExecutionRunner<>(TaskStepInfo.of(
                "StepExecutionRunnerTests",
                "testRunWithNoException"
        ),
                startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor(),
                null
        );
        Assertions.assertNotNull(stepExecutionRunner);
        Optional<String> optionalResult = stepExecutionRunner.overAll().join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testRunWithException() {
        StepExecutionRunner<String> stepExecutionRunner = new StepExecutionRunner<>(TaskStepInfo.of(
                "StepExecutionRunnerTests", "testRunWithException"), startPoint,
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
                    Assertions.assertEquals(message, "Fake Error");
                }
        );
        Assertions.assertNotNull(stepExecutionRunner);
        Optional<String> optionalResult = stepExecutionRunner.overAll().join();
        Assertions.assertTrue(optionalResult.isEmpty());
    }

    @Test
    void testRunWithExceptionNoHandler() {
        StepExecutionRunner<String> stepExecutionRunner = new StepExecutionRunner<>(TaskStepInfo.of(
                "StepExecutionRunnerTests",
                "testRunWithExceptionNoHandler"
        ),
                startPoint,
                CompletableFuture.supplyAsync(() -> {
                    Optional<String> optionalValue = Optional.of(text);
                    if (optionalValue.isPresent()) {
                        throw new RuntimeException("Fake Error");
                    }
                    return optionalValue;
                }),
                Executors.newSingleThreadExecutor(),
                null
        );
        Assertions.assertNotNull(stepExecutionRunner);
        Assertions.assertThrows(RuntimeException.class, () -> {
            stepExecutionRunner.overAll().join();
        });
    }

}
