
package cs505finaltemplate.graphDB;

import cs505finaltemplate.Topics.PatientData;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.VaccineData;
import cs505finaltemplate.Topics.HospPatData;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

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

    public GraphDBEngine(String databaseName) {

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard

        this.databaseName = databaseName;
        OrientDB orient = new OrientDB("remote:ajta238.cs.uky.edu", OrientDBConfig.defaultConfig());
        orient.create(databaseName, ODatabaseType.PLOCAL);
        ODatabaseSession db = orient.open(databaseName, "root", "rootpwd");

        clearDB(db);

        //create classes
        initDB(db);

        db.close();
        orient.close();

    }

    public void initDB(ODatabaseSession db){
        OClass patient = db.getClass("patient");

        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createProperty("testing_id", OType.INTEGER);
            patient.createProperty("patient_mrn", OType.STRING);
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

    //Only used in the /reset function as a part of the API
    public boolean resetDB(){
        try{
            OrientDB database = new OrientDB("remote:ajta238.cs.uky.edu", OrientDBConfig.defaultConfig());

            if(database.exists(databaseName)){
                database.drop(databaseName);
            }
            database.create(databaseName, ODatabaseType.PLOCAL);
            database.close();
            return true;
        }
        catch(Exception e){
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            e.printStackTrace();
            return false;
        }
    }

    private void clearDB(ODatabaseSession db) {

        db.command("DELETE VERTEX FROM patient");
        db.command("DELETE VERTEX FROM hospital");
        db.command("DELETE VERTEX FROM event");
        db.command("DELETE VERTEX FROM vacc");

    }


    public void createContactEdge(PatientData PatientData, OVertex PatientNode, ODatabaseSession database){
        
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

    public void createEventEdge(PatientData PatientData, OVertex PatientNode, ODatabaseSession database){

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
    public void createPatient(PatientData newPatient, ODatabaseSession database){
        
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
        
        createEventEdge(newPatient, PatientBuffer, database);
        createContactEdge(newPatient, PatientBuffer, database);
        
    }

    //Incase of a random json string
    public void createPatient(String jsoString, ODatabaseSession database){
        Gson gson = new Gson();
        PatientData newPatient = gson.fromJson(jsoString, PatientData.class);
        createPatient(newPatient,database);
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
    public void createHospital(HospitalData newhospital, ODatabaseSession database){
        
        String query = "SELECT FROM hospital WHERE hospital_id = ?";
        OResultSet result = database.query(query, newhospital.id);
        OVertex hospitalBuffer;

        if(!result.hasNext())
            hospitalBuffer = database.newVertex("hospital");

        else 
            hospitalBuffer = result.next().getVertex().get();
            
        hospitalBuffer.setProperty("hospital_id", newhospital.id);
        hospitalBuffer.setProperty("hospital_name", newhospital.name);
        hospitalBuffer.setProperty("hospital_address", newhospital.address);
        hospitalBuffer.setProperty("hospital_city", newhospital.city);
        hospitalBuffer.setProperty("hospital_state", newhospital.state);
        hospitalBuffer.setProperty("hospital_type", newhospital.type);
        hospitalBuffer.setProperty("hospital_beds", newhospital.beds);
        hospitalBuffer.setProperty("hospital_county", newhospital.county);
        hospitalBuffer.setProperty("hospital_countyfips", newhospital.countyfips);
        hospitalBuffer.setProperty("hospital_country", newhospital.country);
        hospitalBuffer.setProperty("hospital_latitude", newhospital.latitude);
        hospitalBuffer.setProperty("hospital_longitude", newhospital.longitude);
        hospitalBuffer.setProperty("hospital_type", newhospital.type);
        hospitalBuffer.setProperty("hospital_website", newhospital.website);
        hospitalBuffer.setProperty("hospital_owner", newhospital.owner);
        hospitalBuffer.setProperty("hospital_trauma", newhospital.trauma);
        hospitalBuffer.setProperty("hospital_heli", newhospital.helipad);
        hospitalBuffer.save();
        
        
    }

    //Incase of a random json string
    public void createHospital(String jsoString, ODatabaseSession database){
        Gson gson = new Gson();
        HospitalData newhospital = gson.fromJson(jsoString, HospitalData.class);
        createHospital(newhospital,database);
    }


    public void createVaccPatEdge(VaccineData vaccineData,ODatabaseSession database){
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
    public void createVaccine(VaccineData newvaccine, ODatabaseSession database){
        
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
        

        createVaccPatEdge(newvaccine, database);

    }

    //Incase of a random json string
    public void createVaccine(String jsoString, ODatabaseSession database){
        Gson gson = new Gson();
        VaccineData newvaccine = gson.fromJson(jsoString, VaccineData.class);
        createVaccine(newvaccine,database);
    }


    private void getContacts(ODatabaseSession db, String patient_mrn) {

        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("contact: " + item.getProperty("patient_mrn"));
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    

    
}
