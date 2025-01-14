package cs505finaltemplate;

import cs505finaltemplate.CEP.CEPEngine;
import cs505finaltemplate.Topics.TopicConnector;
import cs505finaltemplate.graphDB.GraphDBEngine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;


public class Launcher {

    public static GraphDBEngine graphDBEngine;
    public static String inputStreamName;
    public static CEPEngine cepEngine;
    public static TopicConnector topicConnector;
    public static final int WEB_PORT = 8082;

    public static String lastCEPOutput = "{}";
    public static List<String> alertlist = new ArrayList<String>();

    public static void main(String[] args) throws IOException {


        //startig DB/CEP init

        //READ CLASS COMMENTS BEFORE USING
        graphDBEngine = new GraphDBEngine("Connections","remote:cajo253.cs.uky.edu","root","rootpwd");
        
        System.out.println("Embedded Database Started...");

        cepEngine = new CEPEngine();

        System.out.println("Starting CEP...");

        inputStreamName = "testInStream";
        String inputStreamAttributesString = "zip_code string";

        String outputStreamName = "testOutStream";
        String outputStreamAttributesString = "zip_code string, count long";

        //This query must be modified.  Currently, it provides the last zip_code and total count
        //You want counts per zip_code, to say another way "grouped by" zip_code
        String queryString = " " +
                "from testInStream#window.timeBatch(5 sec) " +
                "select zip_code, count() as count " +
                "group by zip_code " +
                "insert into testOutStream; ";

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");
        //end DB/CEP Init

        //start message collector
        Map<String,String> message_config = new HashMap<>();
        message_config.put("hostname","cajo253.cs.uky.edu"); //Fill config for your team in
        message_config.put("port","5672"); //
        message_config.put("username","root");
        message_config.put("password","root_pwd");
        message_config.put("virtualhost","/");

        topicConnector = new TopicConnector(message_config);
        topicConnector.connect();
        //end message collector

        //Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505finaltemplate.httpcontrollers");

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
