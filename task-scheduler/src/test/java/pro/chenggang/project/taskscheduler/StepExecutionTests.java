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
 * @author evans
 * @version 1.0.0
 * @since 1.0.0
 */
@Order(0)
public class StepExecutionTests {

    String text = "Tests";

    CompletableFuture<Void> startPoint;

    @BeforeEach
    void beforeEach() {
        startPoint = new CompletableFuture<>();
    }

    @Test
    void testSourceExecutionWithSupplier() {
        SourceExecution<String> sourceExecution = new SourceExecution<>(startPoint,
                () -> Optional.of(text),
                Executors.newSingleThreadExecutor()
        );
        startPoint.complete(null);
        Optional<String> optionalResult = sourceExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testSourceExecutionWithSupplierAndDefaultExecutor() {
        SourceExecution<String> sourceExecution = new SourceExecution<>(startPoint,
                () -> Optional.of(text),
                null
        );
        startPoint.complete(null);
        Optional<String> optionalResult = sourceExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testSourceExecutionWithCompletableFuture() {
        SourceExecution<String> sourceExecution = new SourceExecution<>(startPoint,
                CompletableFuture.supplyAsync(() -> Optional.of(text)),
                Executors.newSingleThreadExecutor()
        );
        startPoint.complete(null);
        Optional<String> optionalResult = sourceExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

    @Test
    void testConverterExecution() {
        ConvertExecution<String, String> sourceExecution = new ConvertExecution<>(startPoint,
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
        DefaultWhenErrorExecution<String> defaultWhenErrorExecution = new DefaultWhenErrorExecution<>(startPoint,
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
        EmptyCheckerExecution<String> emptyCheckerExecution = new EmptyCheckerExecution<>(startPoint,
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
        EmptyCheckerExecution<String> emptyCheckerExecution = new EmptyCheckerExecution<>(startPoint,
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
        FilterExecution<String> filterExecution = new FilterExecution<>(startPoint,
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
        TransformExecution<String, String> transformExecution = new TransformExecution<>(startPoint,
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
        TransformExecution<String, String> transformExecution = new TransformExecution<>(startPoint,
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
        ValidatorExecution<String> validatorExecution = new ValidatorExecution<>(startPoint,
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
        ValidatorExecution<String> validatorExecution = new ValidatorExecution<>(startPoint,
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
        StepExecution<String, String> stepExecution = StepExecution.newStepExecution(startPoint,
                () -> Optional.of(text),
                Executors.newSingleThreadExecutor()
        );
        startPoint.complete(null);
        Optional<String> optionalResult = stepExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }

}
