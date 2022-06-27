package reactor.issue.deadlock.scheduler;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class DeadlockDemoTest {

    ScheduledExecutorService backgroundLoadScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    private final Runnable backgroundLoad = () -> {
        Random random = new Random();
        try (var os = OutputStream.nullOutputStream()) {
            os.write("backgroundTask".getBytes(StandardCharsets.UTF_8));
            Thread.sleep(random.nextInt(9));
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    };

    @Timeout(5)
    @RepeatedTest(100)
    public void test() {
        // to guarantee deadlock in the example use one thread, if you want to see it happens occasionally - add threadCap of the scheduler
        Scheduler scheduler = Schedulers.newBoundedElastic(1, Integer.MAX_VALUE, "gurantees-deadlock-if-one-thread", 30, true);

        // in the real code deadlock is occasional
        // it uses Schedulers.boundedElastic() and occurs only if the external data publishOn task is scheduled on the same thread which is blocked
        // Scheduler thread assignment is based on usage stats so it won't happen in the sterile environment
        // we need to emulate some background load to make usage stats random
        // also we can't use scheduler.schedulePeriodically because it will avoid concurrency with test tasks
        backgroundLoadScheduler.scheduleWithFixedDelay(
            () -> scheduler.schedule(backgroundLoad),
            0, 10, TimeUnit.MILLISECONDS
        );

        String testValue = "testValue";
        Mono.just(testValue)
            .flatMap(value ->
                dataSendTask(scheduler, value)
            )
            .then(Mono.fromCallable(() -> blockingComputationalTask(scheduler, testValue)))
            .doOnNext(s -> System.out.println("Result. Thread name: " + Thread.currentThread().getName() + ", value=" + s))
            .block();
    }

    // in real code this is redis put query - it works on its own pool
    private static Mono<String> dataSendTask(Scheduler scheduler, String value) {
        return Mono.just(value)
            .delayElement(Duration.ofMillis(500))
            .publishOn(scheduler)
            .map(val -> val + "+dataSendTask");
    }

    // in real code this is blocking because it is used also to handle events in sync mode
    private static String blockingComputationalTask(Scheduler scheduler, String value) {
        return Mono.just(value)
            .delayElement(Duration.ofMillis(100))
            .publishOn(scheduler)
            .map(val -> val + "+blockingComputationalTask")
            .block();
    }
}
