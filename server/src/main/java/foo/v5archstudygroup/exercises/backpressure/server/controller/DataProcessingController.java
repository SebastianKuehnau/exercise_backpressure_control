package foo.v5archstudygroup.exercises.backpressure.server.controller;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import foo.v5archstudygroup.exercises.backpressure.server.DataProcessor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.client.HttpServerErrorException;

/**
 * This is the REST controller that receives requests from the client and forwards them to the {@link DataProcessor}.
 * Feel free to change this in any way you like.
 */
@Controller
public class DataProcessingController {

    private final DataProcessor dataProcessor;

    public DataProcessingController(DataProcessor dataProcessor) {
        this.dataProcessor = dataProcessor;
    }

    @PostMapping("/process")
    public ResponseEntity<Void> process(@RequestBody Messages.ProcessingRequest request) {
        return dataProcessor.enqueue(request) ?
            ResponseEntity.ok(null) :
            ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(null);
    }
}
