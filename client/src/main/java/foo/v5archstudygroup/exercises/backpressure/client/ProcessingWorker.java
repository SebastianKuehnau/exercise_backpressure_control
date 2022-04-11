package foo.v5archstudygroup.exercises.backpressure.client;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for sending the messages to the server. You are allowed to change this class in any
 * way you like as long as all messages are delivered successfully to the server.
 */
public class ProcessingWorker {

    private static final long DEFAULT_WAITING_PERIOD_MS = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingWorker.class);
    private final ScheduledExecutorService executorService;
    private final ProcessingRequestGenerator requestGenerator;
    private final RestClient client;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final AtomicInteger sent = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicLong waitingPeriodMs = new AtomicLong(DEFAULT_WAITING_PERIOD_MS);

    public ProcessingWorker(ScheduledExecutorService executorService,
                            ProcessingRequestGenerator requestGenerator,
                            RestClient client) {
        this.executorService = executorService;
        this.requestGenerator = requestGenerator;
        this.client = client;
    }

    public void run() {
        executorService.submit(this::sendNext);
    }

    private void sendNext() {
        if (requestGenerator.hasNext()) {
            sent.incrementAndGet();
            send(requestGenerator.next());
        } else {
            LOGGER.info("Finished sending {} requests with {} errors", sent.get(), errors.get());
            done.set(true);
        }
    }

    private void send(Messages.ProcessingRequest request) {
        try {
            try {
                client.sendToServer(request);
                waitingPeriodMs.set(DEFAULT_WAITING_PERIOD_MS);
                executorService.submit(this::sendNext);
            } catch (HttpClientErrorException ex) {
                if (ex.getStatusCode().equals(HttpStatus.TOO_MANY_REQUESTS)) {
                    var delay = waitingPeriodMs.get();
                    LOGGER.info("Waiting {} ms before retrying to send {}", delay, request.getUuid());
                    waitingPeriodMs.set((long) (delay * 1.2)); // Gradually increase the back-off time
                    executorService.schedule(() -> send(request), delay, TimeUnit.MILLISECONDS);
                } else {
                    throw ex;
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Error sending request {}: {}", request.getUuid(), ex.getMessage());
            errors.incrementAndGet();
        }
    }

    public boolean isDone() {
        return done.get();
    }
}
