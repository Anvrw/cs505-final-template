
package cs505finaltemplate.graphDB;

import cs505finaltemplate.Topics.PatientData;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.VaccineData;
import cs505finaltemplate.Topics.HospPatData;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import com.google.gson.Gson;

public class GraphDBEngine {

    private String databaseName;
    private String databaseLink;
    private String databaseUsrn;
    private String databasePass;

    public GraphDBEngine(String databaseName, String databaseLink, String databaseUsrn, String databasePass) {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard

        this.databaseName = databaseName;
        this.databaseLink = databaseLink;
        this.databaseUsrn = databaseUsrn;
        this.databasePass = databasePass;
        OrientDB orient = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        
	    if(!orient.exists(databaseName))
		    orient.create(databaseName, ODatabaseType.PLOCAL);

        clearDB(orient);

        orient.close();

    }


    //Initializes database
    public void initDB(ODatabaseSession db){
        OClass patient = db.getClass("patient");

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createProperty("testing_id", OType.INTEGER);
            patient.createProperty("patient_name", OType.STRING);
            patient.createProperty("patient_zipcode", OType.INTEGER);
            patient.createProperty("patient_status", OType.INTEGER);
            patient.createProperty("contact_list", OType.LINKSET);
            patient.createProperty("event_list", OType.LINKSET);
            
            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
        }

        OClass hospital = db.getClass("hospital");
        if (hospital == null) {
            hospital = db.createVertexClass("hospital");
        }

        if (patient.getProperty("hospital_id") == null) {
            patient.createProperty("hospital_id", OType.STRING);
            patient.createIndex("hospital_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "hospital_id");
        }

        if (db.getClass("patient_at") == null) {
            db.createEdgeClass("patient_at");
        }

        OClass event = db.getClass("event");
        if (event == null) {
            event = db.createVertexClass("event");
        }

        if (patient.getProperty("event_id") == null) {
            patient.createProperty("event_id", OType.STRING);
            patient.createIndex("event_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "event_id");
        }

        if (db.getClass("participant") == null) {
            db.createEdgeClass("participant");
        }

        OClass vacc = db.getClass("vacc");
        if (vacc == null) {
            vacc = db.createVertexClass("vacc");
        }

        if (patient.getProperty("vacc_id") == null) {
            patient.createProperty("vacc_id", OType.STRING);
            patient.createIndex("vacc_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "vacc_id");
        }

        if (db.getClass("recipient") == null) {
            db.createEdgeClass("recipient");
        }
    }
	


	private void safeQuery(ODatabaseSession Database, String Query){
		try{
		Database.command(Query);
		} catch(Exception e) {
		return;
		}
	}

    //clears the vertexs from each of the data pieces
    private boolean clearDB(OrientDB orient) {

        try{
		ODatabaseSession Database = orient.open(databaseName, databaseUsrn, databasePass);
            String query = "DELETE VERTEX FROM patient";
            safeQuery(Database,query);
	    query = "DELETE VERTEX FROM hospital";
            safeQuery(Database,query);
  	    query = "DELETE VERTEX FROM event";
            safeQuery(Database,query);
	    query = "DELETE VERTEX FROM vacc";
            safeQuery(Database,query);
	    query = "DELETE EDGE FROM contact_with";
            safeQuery(Database,query);
	    query = "DELETE EDGE FROM patient_at";
            safeQuery(Database,query);
  	    query = "DELETE EDGE FROM participant";
            safeQuery(Database,query);
	    query = "DELETE EDGE FROM recipient";
            safeQuery(Database,query);
	
            initDB(Database);
            Database.close();

            return true;
            }
        catch(Exception e){
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            e.printStackTrace();
            return false;
        } 
    }

    //Only used in the /reset function as a part of the API
    public boolean resetDB(){
        
        OrientDB database = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        boolean out = clearDB(database);
        database.close();

        return out;
    }

    //allows for the contact list to be made from queries from the patient dataset
    public void setContactEdge(PatientData PatientData, OVertex PatientNode, ODatabaseSession database){
        
        String queryContactMRNs = "SELECT FROM patient WHERE patient_mrn = ?";
        String queryContactPatEdges = "SELECT FROM contact_with WHERE to = ? AND from = ?";

        for (String item:PatientData.contact_list ){

            OResultSet ContactID = database.query(queryContactMRNs, item);

            OVertex Contact;

            if(ContactID.hasNext()){
                Contact = ContactID.next().getVertex().get();
            } else {
                Contact = database.newVertex("patient");
                Contact.setProperty("patient_mrn", item);
                Contact.save();
            }
            ContactID.close();

            OResultSet ContactPatEdge = database.query(queryContactPatEdges, PatientNode, Contact);
            if(!ContactPatEdge.hasNext()){
                OEdge edge = PatientNode.addEdge(Contact, "contact_with");
                edge.save();
            }
            ContactPatEdge.close();

        }
    }

    public void setEventEdge(PatientData PatientData, OVertex PatientNode, ODatabaseSession database){

        String queryEventIDs = "SELECT FROM event WHERE event_id = ?";
        String queryEventPatEdges = "SELECT FROM participant WHERE to = ? AND from = ?";

        for (String item:PatientData.event_list ){

            OResultSet eventID = database.query(queryEventIDs, item);

            OVertex event;

            if(eventID.hasNext()){
                event = eventID.next().getVertex().get();
            } else {
                event = database.newVertex("event");
                event.setProperty("event_id", item);
                event.save();
            }
            eventID.close();

            OResultSet eventPatEdge = database.query(queryEventPatEdges, PatientNode, event);
            if(!eventPatEdge.hasNext()){
                OEdge edge = PatientNode.addEdge(event, "participant");
                edge.save();
            }
            eventPatEdge.close();


        }
    }

    //general case to add a patient
    public void setPatient(PatientData newPatient, ODatabaseSession database){
        
        String query = "SELECT FROM patient WHERE patient_mrn = ?";
        OResultSet result = database.query(query, newPatient.patient_mrn);

        OVertex PatientBuffer;

        if(!result.hasNext())
            PatientBuffer = database.newVertex("patient");
           
        else 
            PatientBuffer = result.next().getVertex().get();

        PatientBuffer.setProperty("testing_id", newPatient.testing_id);
        PatientBuffer.setProperty("patient_mrn", newPatient.patient_mrn);
        PatientBuffer.setProperty("patient_name", newPatient.patient_name);
        PatientBuffer.setProperty("patient_zipcode", newPatient.patient_zipcode);
        PatientBuffer.setProperty("patient_status", newPatient.patient_status);
        PatientBuffer.setProperty("contact_list", newPatient.contact_list);
        PatientBuffer.setProperty("event_list", newPatient.event_list);
        PatientBuffer.setProperty("patient_status",0);
        PatientBuffer.save();
        
        setEventEdge(newPatient, PatientBuffer, database);
        setContactEdge(newPatient, PatientBuffer, database);
        
    }

    //Incase of a random json string
    public void createPatient(String jsoString, ODatabaseSession database){
        Gson gson = new Gson();
        PatientData newPatient = gson.fromJson(jsoString, PatientData.class);
        setPatient(newPatient,database);
    }

    //Handles HospPatData inputs
    public void addHospPatEdges(HospPatData hospPatData, ODatabaseSession database){
        String queryHospitalIDs = "SELECT FROM hospital WHERE hospital_id = ?";
        String queryPatientMRNs = "SELECT FROM patient WHERE patient_mrn = ?";
        String queryHospPatEdges = "SELECT FROM patient_at WHERE to = ? AND from = ?";
        OResultSet hospitalID = database.query(queryHospitalIDs, hospPatData.hospital_id);
        OResultSet patientID = database.query(queryPatientMRNs, hospPatData.patient_mrn);

        OVertex hospital;
        OVertex patient;

        if(hospitalID.hasNext()){
            hospital = hospitalID.next().getVertex().get();
        } else {
            hospital = database.newVertex("hospital");
            hospital.setProperty("hospital_id", hospPatData.hospital_id);
            hospital.save();
        }
        hospitalID.close();

        if(patientID.hasNext()){
            patient = patientID.next().getVertex().get();
        } else {
            patient = database.newVertex("patient");
            patient.setProperty("patient_mrn", hospPatData.patient_mrn);
            patient.setProperty("patient_status", hospPatData.patient_status);
            patient.setProperty("patient_name", hospPatData.patient_name);
            patient.save();
        }
        patientID.close();
        
        OResultSet HospPatEdges = database.query(queryHospPatEdges, patient, hospital);
        if(!HospPatEdges.hasNext()){
            OEdge edge = patient.addEdge(hospital, "patient_at");
            edge.save();
        }
        HospPatEdges.close();

    }

    //general case to add a hospital
    public void setHospPat(HospPatData newhospital, ODatabaseSession database){
        
        String query = "SELECT FROM hospital WHERE hospital_id = ?";
        OResultSet result = database.query(query, newhospital.hospital_id);
        OVertex hospitalBuffer;

        if(!result.hasNext())
            hospitalBuffer = database.newVertex("hospital");

        else 
            hospitalBuffer = result.next().getVertex().get();

        hospitalBuffer.setProperty("hospital_id", newhospital.hospital_id);

        hospitalBuffer.save();

        addHospPatEdges(null, database);
    }

    //Incase of a random json string
    public void createHospPat(String jsoString, ODatabaseSession database){
        Gson gson = new Gson();
        HospPatData newhospital = gson.fromJson(jsoString, HospPatData.class);
        setHospPat(newhospital,database);
    }

    //allows for the contact list to be made from queries from the vaccine dataset
    public void setVaccPatEdge(VaccineData vaccineData,ODatabaseSession database){
        String queryVaccineIDs = "SELECT FROM hospital WHERE hospital_id = ?";
        String queryPatientMRNs = "SELECT FROM patient WHERE patient_mrn = ?";
        String queryVaccPatEdges = "SELECT FROM patient_at WHERE to = ? AND from = ?";
        OResultSet vaccineID = database.query(queryVaccineIDs, vaccineData.vaccination_id);
        OResultSet patientID = database.query(queryPatientMRNs, vaccineData.patient_mrn);

        OVertex vaccine;
        OVertex patient;

        if(vaccineID.hasNext()){
            vaccine = vaccineID.next().getVertex().get();
        } else {
            vaccine = database.newVertex("vaccine");
            vaccine.setProperty("vaccine_id", vaccineData.vaccination_id);
            vaccine.save();
        }
        vaccineID.close();

        if(patientID.hasNext()){
            patient = patientID.next().getVertex().get();
        } else {
            patient = database.newVertex("patient");
            patient.setProperty("patient_mrn", vaccineData.patient_mrn);
            patient.setProperty("patient_name", vaccineData.patient_name);
            patient.save();
        }
        patientID.close();
        
        OResultSet HospPatEdge = database.query(queryVaccPatEdges, patient, vaccine);
        if(!HospPatEdge.hasNext()){
            OEdge edge = patient.addEdge(vaccine, "recipient");
            edge.save();
        }
        HospPatEdge.close();

    }

    //general case to add a vaccine
    public void setVaccine(VaccineData newvaccine, ODatabaseSession database){
        
        String query = "SELECT FROM vaccine WHERE vaccine_id = ?";
        OResultSet result = database.query(query, newvaccine.patient_mrn);
        OVertex vaccineBuffer;

        if(!result.hasNext())
            vaccineBuffer = database.newVertex("vaccine");
        else 
            vaccineBuffer = result.next().getVertex().get();

        vaccineBuffer.setProperty("testing_id", newvaccine.vaccination_id);
        vaccineBuffer.setProperty("patient_mrn", newvaccine.patient_mrn);
        vaccineBuffer.setProperty("patient_name", newvaccine.patient_name);
        vaccineBuffer.save();
        

        setVaccPatEdge(newvaccine, database);

    }

    //Incase of a random json string
    public void createVaccine(String jsoString, ODatabaseSession database){
        Gson gson = new Gson();
        VaccineData newvaccine = gson.fromJson(jsoString, VaccineData.class);
        setVaccine(newvaccine,database);
    }

    //General input handler for all forms of json input
    public void jsoInputHandler(String message, Character type){

        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, databaseUsrn, databasePass);


        switch(type){
            case 'h':
                createHospPat(message,database);
            break;

            case 'p':
                createPatient(message, database);
            break;

            case 'v':
                createVaccine(message, database);
            break;

            default:
            break;
        }

        database.close();
        orient.close();
    }

    //Traverses each patient connection to a given patient and marks them down for output
    public List<String> getContacts(String patient_mrn) {

        //Variable setup
        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, databaseUsrn, databasePass);

        String queryTraversal = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?)" +
                "WHILE $depth <= 2";
        String queryPatient = "SELECT FROM patient WHERE patient_mrn = ?";

        try{
        OResultSet traversal = database.query(queryTraversal, patient_mrn);
        OResultSet patient = database.query(queryPatient, patient_mrn);

        //Error check; return empty string
        if(!traversal.hasNext()){
            traversal.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();

            List<String> errOut = new ArrayList<>();
            errOut.add("");
            return errOut;
        }
        patient.close();

        List<String> contactList = new ArrayList<>();

        //traverse each patient contact node
        while (traversal.hasNext()) {
            OResult item = traversal.next();
            contactList.add(item.getProperty("patient_mrn"));
        }

        traversal.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();

        return contactList;
    } catch(Exception e){
        database.close();
        orient.close();

        List<String> errOut = new ArrayList<>();
        errOut.add("-1");
        return errOut;
    }
    }

    //Traverses each event connection to a given patient and marks down all other
    // patients connected to it for output
    public List<String> getEventContacts(String patient_mrn) {

        //Variable setup
        OrientDB orient;
        ODatabaseSession database;
        Gson gson = new Gson();
        orient = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, databaseUsrn, databasePass);

        String queryTravPat = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        String queryTravEvent = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from event where event_id = ?) " +
                "WHILE $depth <= 2";
        String queryPatient = "SELECT FROM patient WHERE patient_mrn = ?";

        try{
        OResultSet travPat = database.query(queryTravPat, patient_mrn);
        OResultSet patient = database.query(queryPatient, patient_mrn);

        //Error check; return value 0 and an empty array mapped to it
        if(!travPat.hasNext()){
            travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();

            List<String> errOut = new ArrayList<>();
            errOut.add("[{000:[]}]");
            return errOut;
        }
        patient.close();

        List<String> contactList = new ArrayList<>();
        DecimalFormat threeSigForm = new DecimalFormat("000");

        //traverse each event contact node
        int i = 1;
        while (travPat.hasNext()) {
           
            OResult patItem = travPat.next();

            if(patItem.hasProperty("event_id") && patItem.getProperty("event_id") != ""){
                HashMap<String,String> shortMap = new HashMap<>();
                List<String> shortList = new ArrayList<>();

                String eventID = patItem.getProperty("event_id");
                OResultSet travEvent = database.query(queryTravEvent,eventID);
                
                //traverse each patient contact node
                while(travEvent.hasNext()){
                    OResult patEvent = travEvent.next();

                    if(patEvent.hasProperty("patient_mrn") 
                    && !patient_mrn.equals(patEvent.getProperty("patient_mrn"))){
                        shortList.add(patEvent.getProperty("patient_mrn"));
                    }
                }
                travEvent.close();
                shortMap.put(threeSigForm.format(i),gson.toJson(shortList));
                contactList.add(gson.toJson(shortMap));
            }  
        }

        travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();

        return contactList;

    } catch(Exception e){
        database.close();
        orient.close();

        List<String> errOut = new ArrayList<>();
        errOut.add("[{000:[-1]}]");
        return errOut;
    }
    }

    //This method receives the total number of patients in a given hospital and 
    // calculates out the percent vaccinated, outputting both at the end
    public HashMap<String,String> getHospitalInfo(String hospital_id) {

        //Variable setup
        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, databaseUsrn, databasePass);

        String queryTravHosp = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from hospital where id = ?) " +
                "WHILE $depth <= 2";
        String queryTravPat = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        try{
        OResultSet travHosp = database.query(queryTravHosp, hospital_id);

        int inPatientCount = 0;
        int icuPatientCount = 0;
        int ventPatientCount = 0;
        int vaccInPatientCount = 0;
        int vaccIcuPatientCount = 0;
        int vaccVentPatientCount = 0;

        //Error check; If the hospital asked for doesnt exist, return -1's
        if(!travHosp.hasNext()){
            travHosp.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();

            HashMap<String,String> errOut = new HashMap<String,String>();

            errOut.put("in-patient_count",String.valueOf("-1"));
            errOut.put("in-patient_vax",String.valueOf("-1"));

            errOut.put("icu-patient_count",String.valueOf("-1"));
            errOut.put("icu-patient_vax",String.valueOf("-1"));

            errOut.put("patient_vent_count",String.valueOf("-1"));
            errOut.put("patient_vent_vax",String.valueOf("-1"));

            return errOut;
        }

        HashMap<String,String> hospList = new HashMap<>();

        //Traverse each patient attached to the hospital
        while (travHosp.hasNext()) {
           
            OResult patItem = travHosp.next();

            if(patItem.hasProperty("patient_mrn") && patItem.getProperty("patient_mrn") != ""){
                //Begin marking down patient status
                int status = patItem.getProperty("patient_status");

                String patID = patItem.getProperty("patient_mrn");
                OResultSet travPat = database.query(queryTravPat,patID);
                
                int vaccStatus = 0;
                while(travPat.hasNext()){
                    OResult vaccItem = travPat.next();

                    if(vaccItem.hasProperty("testing_id") 
                    && patID.equals(vaccItem.getProperty("patient_mrn"))
                    && vaccItem.getProperty("testing_id") != ""){
                        vaccStatus = 3;
                    }
                }
                travPat.close();

                //determine which case something is
                switch(status+vaccStatus){
                    case 1:
                        inPatientCount+=1;
                    break;
                    case 2:
                        icuPatientCount+=1;
                    break;
                    case 3:
                        ventPatientCount+=1;
                    break;
                    case 4:
                        vaccInPatientCount+=1;
                    break;
                    case 5:
                        vaccIcuPatientCount+=1;
                    break;
                    case 6:
                        vaccVentPatientCount+=1;
                    break;
                    default:
                    break;
                }
            }  
        }
    
        travHosp.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();
    

        //Format output to what is desired.

        DecimalFormat percForm = new DecimalFormat(".00");

        hospList.put("in-patient_count",String.valueOf(inPatientCount));
        String vaccInPerc = String.valueOf(((double) vaccInPatientCount)/((double) inPatientCount));
        hospList.put("in-patient_vax",percForm.format(vaccInPerc));

        hospList.put("icu-patient_count",String.valueOf(icuPatientCount));
        String vaccIcuPerc = String.valueOf(((double) vaccIcuPatientCount)/((double) icuPatientCount));
        hospList.put("icu-patient_vax",percForm.format(vaccIcuPerc));

        hospList.put("patient_vent_count",String.valueOf(ventPatientCount));
        String vaccVentPerc = String.valueOf(((double) vaccVentPatientCount)/((double) ventPatientCount));
        hospList.put("patient_vent_vax",percForm.format(vaccVentPerc));

        return hospList;
    }catch(Exception e){
        database.close();
        orient.close();

        HashMap<String,String> errOut = new HashMap<String,String>();

        errOut.put("in-patient_count",String.valueOf("-1"));
        errOut.put("in-patient_vax",String.valueOf("-1"));

        errOut.put("icu-patient_count",String.valueOf("-1"));
        errOut.put("icu-patient_vax",String.valueOf("-1"));

        errOut.put("patient_vent_count",String.valueOf("-1"));
        errOut.put("patient_vent_vax",String.valueOf("-1"));

        return errOut;
    }
    }

    //This method receives the total number of patients in hospitals for all hospitals
    // and calculates out the percent vaccinated, outputting both at the end
    public HashMap<String,String> getTotalHospitalInfo() {

        //Variable setup
        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB(databaseLink, databaseUsrn, databasePass, OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, databaseUsrn, databasePass);

        String queryTravHosp = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from hospital where id = ?) " +
                "WHILE $depth <= 2";
        String queryTravPat = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        String queryHospitals = "SELECT FROM hospital ORDER BY id DESC";

        try{
        int inPatientCount = 0;
        int icuPatientCount = 0;
        int ventPatientCount = 0;
        int vaccInPatientCount = 0;
        int vaccIcuPatientCount = 0;
        int vaccVentPatientCount = 0;

        OResultSet hospitals = database.query(queryHospitals, "");

        //Incase of no hospitals, return -1's
        if(!hospitals.hasNext()){
            hospitals.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();
            HashMap<String,String> errOut = new HashMap<String,String>();

            errOut.put("in-patient_count",String.valueOf("-1"));
            errOut.put("in-patient_vax",String.valueOf("-1"));

            errOut.put("icu-patient_count",String.valueOf("-1"));
            errOut.put("icu-patient_vax",String.valueOf("-1"));

            errOut.put("patient_vent_count",String.valueOf("-1"));
            errOut.put("patient_vent_vax",String.valueOf("-1"));

            return errOut;
        }

        //Traverse each hospital in the database

        while(hospitals.hasNext()){
        
        OResult currHospital = hospitals.next();
        String hospital_id = currHospital.getProperty("id");

        OResultSet travHosp = database.query(queryTravHosp, hospital_id);

        //Traverse each patient connected to the hospital

        while (travHosp.hasNext()) {
           
            OResult patItem = travHosp.next();

            if(patItem.hasProperty("patient_mrn") && patItem.getProperty("patient_mrn") != ""){
                int status = patItem.getProperty("patient_status");

                //Begin marking down patient status

                String patID = patItem.getProperty("patient_mrn");
                OResultSet travPat = database.query(queryTravPat,patID);
                
                int vaccStatus = 0;
                while(travPat.hasNext()){
                    OResult vaccItem = travPat.next();

                    if(vaccItem.hasProperty("testing_id") 
                    && patID.equals(vaccItem.getProperty("patient_mrn"))
                    && vaccItem.getProperty("testing_id") != ""){
                        vaccStatus = 3;
                    }
                }
                travPat.close();

                //determine which case something is

                switch(status+vaccStatus){
                    case 1:
                        inPatientCount+=1;
                    break;
                    case 2:
                        icuPatientCount+=1;
                    break;
                    case 3:
                        ventPatientCount+=1;
                    break;
                    case 4:
                        vaccInPatientCount+=1;
                    break;
                    case 5:
                        vaccIcuPatientCount+=1;
                    break;
                    case 6:
                        vaccVentPatientCount+=1;
                    break;
                    default:
                    break;
                }
            }  
        }
    
        travHosp.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        }
        hospitals.close();
        database.close();
        orient.close();

        //Generate the output in its correct format and decimal count

        HashMap<String,String> hospList = new HashMap<>();

        DecimalFormat percForm = new DecimalFormat(".00");

        hospList.put("in-patient_count",String.valueOf(inPatientCount));
        double vaccInPerc = ((double) vaccInPatientCount)/((double) inPatientCount);
        hospList.put("in-patient_vax",percForm.format(vaccInPerc));

        hospList.put("icu-patient_count",String.valueOf(icuPatientCount));
        double vaccIcuPerc = ((double) vaccIcuPatientCount)/((double) icuPatientCount);
        hospList.put("icu-patient_vax",percForm.format(vaccIcuPerc));

        hospList.put("patient_vent_count",String.valueOf(ventPatientCount));
        double vaccVentPerc = ((double) vaccVentPatientCount)/((double) ventPatientCount);
        hospList.put("patient_vent_vax",percForm.format(vaccVentPerc));

        return hospList;
    } catch(Exception e){
        database.close();
        orient.close();
        HashMap<String,String> errOut = new HashMap<String,String>();

        errOut.put("in-patient_count",String.valueOf("-1"));
        errOut.put("in-patient_vax",String.valueOf("-1"));

        errOut.put("icu-patient_count",String.valueOf("-1"));
        errOut.put("icu-patient_vax",String.valueOf("-1"));

        errOut.put("patient_vent_count",String.valueOf("-1"));
        errOut.put("patient_vent_vax",String.valueOf("-1"));

        return errOut;
    }
    }
    
}
