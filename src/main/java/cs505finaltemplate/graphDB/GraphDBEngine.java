
package cs505finaltemplate.graphDB;

import cs505finaltemplate.Topics.PatientData;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.VaccineData;

import java.io.PrintWriter;
import java.io.StringWriter;

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

        if (db.getClass("distributor") == null) {
            db.createEdgeClass("distributor");
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

    //general case to add a patient
    public void setPatient(PatientData newPatient){
        
        ODatabaseSession database;
        OrientDB orient = new OrientDB("remote:ajta238.cs.uky.edu", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");
        String query = "SELECT FROM patient WHERE patient_mrn = ?";
        OResultSet result = database.query(query, newPatient.patient_mrn);

        if(!result.hasNext()){
            OVertex PatientBuffer = database.newVertex("patient");
            PatientBuffer.setProperty("testing_id", newPatient.testing_id);
            PatientBuffer.setProperty("patient_mrn", newPatient.patient_mrn);
            PatientBuffer.setProperty("patient_name", newPatient.patient_name);
            PatientBuffer.setProperty("patient_zipcode", newPatient.patient_zipcode);
            PatientBuffer.setProperty("patient_status", newPatient.patient_status);
            PatientBuffer.setProperty("contact_list", newPatient.contact_list);
            PatientBuffer.setProperty("event_list", newPatient.event_list);
            PatientBuffer.setProperty("patient_status",0);
            PatientBuffer.save();
        }

        else if (result.next().isVertex()){
            OVertex PatientBuffer = result.next().getVertex().get();
            PatientBuffer.setProperty("testing_id", newPatient.testing_id);
            PatientBuffer.setProperty("patient_mrn", newPatient.patient_mrn);
            PatientBuffer.setProperty("patient_name", newPatient.patient_name);
            PatientBuffer.setProperty("patient_zipcode", newPatient.patient_zipcode);
            PatientBuffer.setProperty("patient_status", newPatient.patient_status);
            PatientBuffer.setProperty("contact_list", newPatient.contact_list);
            PatientBuffer.setProperty("event_list", newPatient.event_list);
            PatientBuffer.setProperty("patient_status",0);
            PatientBuffer.save();
        }
        
        orient.close();
    }

    


    //Incase of a random json string
    public void setPatient(String jsoString){
        Gson gson = new Gson();
        PatientData newPatient = gson.fromJson(jsoString, PatientData.class);
        setPatient(newPatient);
    }
    public void createHospitalJson(String jsoString){
        Gson gson = new Gson();
        HospitalData newHospital = gson.fromJson(jsoString, HospitalData.class);
        setHospital(newHospital);
    }

    public void createVaccineJson(String jsoString){
        Gson gson = new Gson();
        VaccineData newVaccine = gson.fromJson(jsoString, VaccineData.class);
        setVaccine(newVaccine);
    }


    //general case to add a hospital
    public void setHospital(HospitalData newhospital){
        
        ODatabaseSession database;
        OrientDB orient = new OrientDB("remote:ajta238.cs.uky.edu", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");
        String query = "SELECT FROM hospital WHERE hospital_mrn = ?";
        OResultSet result = database.query(query, newhospital.id);

        if(!result.hasNext()){
            OVertex hospitalBuffer = database.newVertex("hospital");
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

        else if (result.next().isVertex()){
            OVertex hospitalBuffer = result.next().getVertex().get();
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
        
        orient.close();
    }

    //Incase of a random json string
    public void setHospital(String jsoString){
        Gson gson = new Gson();
        HospitalData newhospital = gson.fromJson(jsoString, HospitalData.class);
        setHospital(newhospital);
    }


    //general case to add a vaccine
    public void setvaccine(VaccineData newvaccine){
        
        ODatabaseSession database;
        OrientDB orient = new OrientDB("remote:ajta238.cs.uky.edu", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");
        String query = "SELECT FROM vaccine WHERE vaccine_mrn = ?";
        OResultSet result = database.query(query, newvaccine.patient_mrn);

        if(!result.hasNext()){
            OVertex vaccineBuffer = database.newVertex("vaccine");
            vaccineBuffer.setProperty("testing_id", newvaccine.vaccination_id);
            vaccineBuffer.setProperty("patient_mrn", newvaccine.patient_mrn);
            vaccineBuffer.setProperty("patient_name", newvaccine.patient_name);
            vaccineBuffer.save();
        }

        else if (result.next().isVertex()){
            OVertex vaccineBuffer = result.next().getVertex().get();
            vaccineBuffer.setProperty("testing_id", newvaccine.vaccination_id);
            vaccineBuffer.setProperty("patient_mrn", newvaccine.patient_mrn);
            vaccineBuffer.setProperty("patient_name", newvaccine.patient_name);
            vaccineBuffer.save();
        }
        
        orient.close();
    }

    //Incase of a random json string
    public void setvaccine(String jsoString){
        Gson gson = new Gson();
        VaccineData newvaccine = gson.fromJson(jsoString, VaccineData.class);
        setvaccine(newvaccine);
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

    private void clearDB(ODatabaseSession db) {

        db.command("DELETE VERTEX FROM patient");
        db.command("DELETE VERTEX FROM hospital");
        db.command("DELETE VERTEX FROM event");
        db.command("DELETE VERTEX FROM vacc");

    }

}
