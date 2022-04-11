package foo.v5archstudygroup.exercises.backpressure.client;

import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.Executors;

/**
 * Main entry point into the client application.
 */
public class BackpressureClientApp {

    // This value should be high enough to cause trouble without any kind of back pressure control. Tweak it if needed
    // to get a suitable amount of trouble to begin with.
    private static final int MESSAGE_COUNT = 300;

    // Once you have gotten everything to work with one worker, increase this to 2 and see if the program still works.
    private static final int WORKERS = 2;

    public static void main(String[] args) throws Exception {
        var client = new RestClient(URI.create("http://localhost:8080"));

        var executorService = Executors.newScheduledThreadPool(WORKERS);
        var workers = new ArrayList<ProcessingWorker>();
        for (int i = 0; i < WORKERS; ++i) {
            var requestGenerator = new ProcessingRequestGenerator(MESSAGE_COUNT);
            var worker = new ProcessingWorker(executorService, requestGenerator, client);
            workers.add(worker);
            worker.run();
        }

        // This is ugly. Don't do this in production software.
        while (workers.stream().anyMatch(worker -> !worker.isDone())) {
            Thread.sleep(100);
        }

        // Finally stop the threads
        executorService.shutdown();
    }
}
