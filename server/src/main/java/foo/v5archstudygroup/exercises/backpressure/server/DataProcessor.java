package foo.v5archstudygroup.exercises.backpressure.server;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the class that simulates the data processing.
 */
@Service
public class DataProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataProcessor.class);

    /**
     * You are not allowed to increase the number of threads, but otherwise you can do whatever changes you want
     * to the executor service.
     */
    private final ExecutorService threadPool = new ThreadPoolExecutor(10, 10, 0, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(100)); // SOLUTION: Use an upper limit on the work queue. That way you will start rejecting jobs before you run out of memory.
    private final AtomicLong processed = new AtomicLong();

    @PreDestroy
    void destroy() {
        // Always remember to clean up your threads
        threadPool.shutdown();
    }

    public void enqueue(Messages.ProcessingRequest request) {
        threadPool.submit(() -> doProcess(request));
    }

    /**
     * You are not allowed to change this method!
     */
    private void doProcess(Messages.ProcessingRequest request) {
        try {
            // Pretend we are doing something really heavy with this request. The more bytes we have, the longer it takes
            // to complete
            Thread.sleep(request.getPayload().size() / 8192);
            LOGGER.info("Done processing request {}. Processed: {}", request.getUuid(), processed.incrementAndGet());
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
}
