package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.graphDB.GraphDBEngine;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }


    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam() {
        String responseString = "{}";
        try {
            System.out.println("WHAT");
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "Persuasive Trees");
            responseMap.put("Team_members_sids", "[912297061,912267407]");

            if (Launcher.graphDBEngine != null && Launcher.cepEngine != null) {
                responseMap.put("app_status_code", String.valueOf(1));
            } else {
                responseMap.put("app_status_code", String.valueOf(0));
            }
            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        try{

            boolean resGraph = Launcher.graphDBEngine.resetDB();
            boolean resCEP = Launcher.cepEngine.resetDB();

            Map<String,String> responseMap = new HashMap<>();
            if(resCEP&&resGraph){
                responseMap.put("reset_status_code","1");
            }else{
                responseMap.put("reset_status_code","0");
            }
            
            responseString = gson.toJson(responseMap);
        }
        catch (Exception ex){
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response zipalertlist() {
        String responseString = "{}";
        try{
            Map<String,List<String>> responseMap = new HashMap<>();
            responseMap.put("ziplist",Launcher.alertlist);
            responseString = gson.toJson(responseMap);
        }
        catch (Exception ex){
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertlist() {
        String responseString = "{}";
        try{
            Map<String,String> responseMap = new HashMap<>();

            if(Launcher.alertlist.size() >= 5){
                responseMap.put("state_status","1");
            }else{
                responseMap.put("state_status","0");
            }
            responseString = gson.toJson(responseMap);
        }
        catch (Exception ex){
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getconfirmedcontacts/{patient_mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getconfirmedcontacts(@PathParam("patient_mrn") String patientMRN) {
        String responseString = "{}";
        try{
            Map<String,List<String>> responseMap = new HashMap<>();
            responseString = gson.toJson(responseMap);
            responseString = gson.toJson(Launcher.graphDBEngine.getContacts(patientMRN));
        }
        catch (Exception ex){
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpossiblecontacts/{patient_mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getpossiblecontacts(@PathParam("patient_mrn") String patientMRN) {
        String responseString = "{}";
        try{
            Map<String,List<String>> responseMap = new HashMap<>();
            responseString = gson.toJson(responseMap);
            responseString = gson.toJson(Launcher.graphDBEngine.getEventContacts(patientMRN));
        }
        catch (Exception ex){
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getpatientstatus(@PathParam("hospital_id") String hospitalID) {
        String responseString = "{}";
        try{

            responseString = gson.toJson(Launcher.graphDBEngine.getHospitalInfo(hospitalID));
        }
        catch (Exception ex){

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getpatientstatuses(@PathParam("hospital_id") String hospitalID) {
        String responseString = "{}";
        try{
            responseString = gson.toJson(Launcher.graphDBEngine.getTotalHospitalInfo());
        }
        catch (Exception ex){

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getlastcep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("lastoutput",Launcher.lastCEPOutput);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }


}
