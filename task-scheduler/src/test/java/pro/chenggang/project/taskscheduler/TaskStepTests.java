package pro.chenggang.project.taskscheduler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * @author evans
 * @version 1.0.0
 * @since 1.0.0
 */
@Order(2)
public class TaskStepTests {

    String text = "Tests";

    @Test
    void testSourceStep(){
        SourceStep<String> sourceStep = new SourceStep<>(() -> Optional.of(text), null);
        Supplier<Optional<String>> source = sourceStep.source();
        Optional<String> optionalResult = source.get();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(),text);
    }

    @Test
    void testNewTaskStep(){
        TaskStep<String, String> taskStep = TaskStep.newTaskStep(() -> Optional.of(text), null);
        Supplier<Optional<String>> source = taskStep.source();
        Optional<String> optionalResult = source.get();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(),text);
    }

    @Test
    void testToStepExecution(){
        StepExecution<String, String> stepExecution = TaskStep.newTaskStep(() -> Optional.of(text), null)
                .stepExecution();
        stepExecution.startPoint.complete(null);
        Optional<String> optionalResult = stepExecution.then()
                .join();
        Assertions.assertTrue(optionalResult.isPresent());
        Assertions.assertEquals(optionalResult.get(), text);
    }
}
