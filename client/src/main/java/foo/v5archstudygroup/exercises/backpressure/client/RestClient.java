package foo.v5archstudygroup.exercises.backpressure.client;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import foo.v5archstudygroup.exercises.backpressure.messages.converter.ProcessingRequestMessageConverter;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.List;

/**
 * This class is responsible for interacting with the REST endpoint on the server. You are allowed to change this class
 * in any way you like.
 */
public class RestClient {

    private final RestTemplate restTemplate;
    private final URI serverUri;

    public RestClient(URI serverUri) {
        var requestFactory = new SimpleClientHttpRequestFactory();
        // Always remember to set timeouts!
        requestFactory.setConnectTimeout(100);
        requestFactory.setReadTimeout(1000);
        restTemplate = new RestTemplate(List.of(new ProcessingRequestMessageConverter()));
        restTemplate.setRequestFactory(requestFactory);
        this.serverUri = serverUri;
    }

    public void sendToServer(Messages.ProcessingRequest processingRequest) {
        var uri = UriComponentsBuilder.fromUri(serverUri).path("/process").build().toUri();
        var throwException = true ;

        while (throwException) {
            throwException = false ;
            try {
                restTemplate.postForEntity(uri, processingRequest, Void.class);
            } catch (Exception ex) {
                throwException = true ;
            }
        }
    }
}
