
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

    public void jsoInputHandler(String message, Character type){

        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB("remote:ajta238.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");


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

    public String getContacts(String patient_mrn) {

        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB("remote:ajta238.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");

        String queryTraversal = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) c" +
                "WHILE $depth <= 2";
        String queryPatient = "SELECT FROM patient WHERE patient_mrn = ?";

        OResultSet traversal = database.query(queryTraversal, patient_mrn);
        OResultSet patient = database.query(queryPatient, patient_mrn);

        if(!traversal.hasNext()){
            traversal.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();
            if(!patient.hasNext()){
                patient.close();
                return "Patient has no contacts.";
            } else {
                patient.close();
                return "Patient does not exist.";
            }
        }
        patient.close();

        List<String> contactList = new ArrayList<>();

        while (traversal.hasNext()) {
            OResult item = traversal.next();
            contactList.add(item.getProperty("patient_mrn"));
        }

        traversal.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();

        Gson gson = new Gson();
        return gson.toJson(contactList);
    }

    public String getEventContacts(String patient_mrn) {

        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB("remote:ajta238.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");

        String queryTravPat = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from patient where patient_mrn = ?) " +
                "WHILE $depth <= 2";
        String queryTravEvent = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from event where event_id = ?) " +
                "WHILE $depth <= 2";
        String queryPatient = "SELECT FROM patient WHERE patient_mrn = ?";

        OResultSet travPat = database.query(queryTravPat, patient_mrn);
        OResultSet patient = database.query(queryPatient, patient_mrn);

        if(!travPat.hasNext()){
            travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();
            if(!patient.hasNext()){
                patient.close();
                return "Patient has no contacts.";
            } else {
                patient.close();
                return "Patient does not exist.";
            }
        }
        patient.close();

        List<HashMap<String,List<String>>> contactList = new ArrayList<>();

        int i = 1;
        while (travPat.hasNext()) {
           
            OResult eventItem = travPat.next();

            if(eventItem.hasProperty("event_id") && eventItem.getProperty("event_id") != ""){
                HashMap<String,List<String>> shortMap = new HashMap<>();
                List<String> shortList = new ArrayList<>();

                String eventID = eventItem.getProperty("event_id");
                OResultSet travEvent = database.query(queryTravEvent,eventID);
                
                while(travEvent.hasNext()){
                    OResult patItem = travEvent.next();

                    if(patItem.hasProperty("patient_mrn") 
                    && !patient_mrn.equals(patItem.getProperty("patient_mrn"))){
                        shortList.add(patItem.getProperty("patient_mrn"));
                    }
                }
                shortMap.put(String.valueOf(i),shortList);
                contactList.add(shortMap);
            }  
        }

        travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();

        Gson gson = new Gson();
        return gson.toJson(contactList);
    }

    public String getEventContacts(String hospital_id) {

        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB("remote:ajta238.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");

        String queryTravPat = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from hospital where id = ?) " +
                "WHILE $depth <= 2";
        String queryTravEvent = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from event where event_id = ?) " +
                "WHILE $depth <= 2";
        String queryHospital = "SELECT FROM hospital WHERE id = ?";

        OResultSet travPat = database.query(queryTravPat, hospital_id);
        OResultSet hospitalID = database.query(queryPat, hospital_id);

        if(!travPat.hasNext()){
            travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();
            if(!hospital.hasNext()){
                patient.close();
                return "Hospital has no contacts.";
            } else {
                patient.close();
                return "Hospital does not exist.";
            }
        }
        hosptial.close();

        int inPatientCount = 0;
        int icuPatientCount = 0;
        int ventPatientCount = 0;
        int vaccinatedInPatientCount = 0;
        int vaccinatedIcuPatientCount = 0;
        int vaccinatedVentPatientCount = 0;

        List<HashMap<String,List<String>>> contactList = new ArrayList<>();

        int i = 1;
        while (travPat.hasNext()) {

            OResult eventItem = travPat.next();

            if(eventItem.hasProperty("event_id") && eventItem.getProperty("event_id") != ""){
                HashMap<String,List<String>> shortMap = new HashMap<>();
                List<String> shortList = new ArrayList<>();

                String eventID = eventItem.getProperty("event_id");
                OResultSet travEvent = database.query(queryTravEvent,eventID);

                while(travEvent.hasNext()){
                    OResult patItem = travEvent.next();

                    if(patItem.hasProperty("patient_mrn"){
                        if(patItem.hasProperty("patient_status")){
                            String patientStatus = patItem.getProperty("patient_status");

                            if(patientStatus.equals("in-patient")){
                                inPatientCount++;

                                if(patItem.hasProperty("vaccination_status") && patItem.getProperty("vaccination_status").equals("vaccinated")){
                                    vaccinatedInPatientCount++;
                                }
                            }else if(patientStatus.equals("icu-patient")){
                                icuPatientCount++;
                                if(patItem.hasProperty("vaccination_status") && patItem.getProperty("vaccination_status").equals("vaccinated")){
                                    vaccinatedIcuPatientCount++;
                                }
                            }else if(patientStatus.equals("patient-vent")){
                                ventPatientCount++;
                                if(patItem.hasPropety("vaccination_status") && patItem.getProperty("vaccination_status").equals(vaccinated)){
                                    vaccinatedVentPatientCount++;
                                }
                            }
                        }
                    }
                }
                shortMap.put(String.valueOf(i),shortList);
                contactList.add(shortMap);
            }
        }

        travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();

        double inPatientVaxPercentage = inPatientCount > 0 ? (double) vaccinatedInPatientCount / inPatientCount : 0;
        double icuPatientVaxPercentage = icuPatientCount > 0 ? (double) vaccinatedIcuPatientCount / icuPatientCount : 0;
        double patientVentVaxPercentage = patientVentCount > 0 ? (double) vaccinatedVentPatientCount / ventPatientCount : 0;


        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("hospital_id", hospital_id);
        jsonObject.addProperty("in_patient_count", inPatientCount);
        jsonObject.addProperty("icu_patient_count", icuPatientCount);
        jsonObject.addProperty("patient_vent_count", ventPatientCount);
        jsonObject.addProperty("vaccinated_in_patients", vaccinatedInPatientCount);
        jsonObject.addProperty("vaccinated_icu_patients", vaccinatedIcuPatientCount);
        jsonObject.addProperty("vaccinated_patients_on_vent", vaccinatedVentPatientCount);
        jsonObject.addProperty("in_patient_vax_percentage", inPatientVaxPercentage);
        jsonObject.addProperty("icu_patient_vax_percentage", icuPatientVaxPercentage);
        jsonObject.addProperty("patient_vent_vax_percentage", patientVentVaxPercentage);

        return jsonObject.toString();

        Gson gson = new Gson();
        return gson.toJson(contactList);
    }

    public String getEventContacts(String hospital_id) {

        OrientDB orient;
        ODatabaseSession database;
        orient = new OrientDB("remote:ajta238.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        database = orient.open(databaseName, "root", "rootpwd");

        String queryTravPat = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from hospital where id = ?) " +
                "WHILE $depth <= 2";
        String queryTravEvent = "TRAVERSE inE(), outE(), inV(), outV() " +
                "FROM (select from event where event_id = ?) " +
                "WHILE $depth <= 2";
        String queryHospital = "SELECT *FROM hospital WHERE id = ?";

        OResultSet travPat = database.query(queryTravPat, hospital_id);
        OResultSet hospitalID = database.query(queryPat, hospital_id);

        if(!travPat.hasNext()){
            travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
            database.close();
            orient.close();
            if(!hospital.hasNext()){
                patient.close();
                return "Hospital has no contacts.";
            } else {
                patient.close();
                return "Hospital does not exist.";
            }
        }
        hosptial.close();

        int inPatientCount = 0;
        int icuPatientCount = 0;
        int ventPatientCount = 0;
        int vaccinatedInPatientCount = 0;
        int vaccinatedIcuPatientCount = 0;
        int vaccinatedVentPatientCount = 0;

        List<HashMap<String,List<String>>> contactList = new ArrayList<>();

        int i = 1;
        while (travPat.hasNext()) {

            OResult eventItem = travPat.next();

            if(eventItem.hasProperty("event_id") && eventItem.getProperty("event_id") != ""){
                HashMap<String,List<String>> shortMap = new HashMap<>();
                List<String> shortList = new ArrayList<>();

                String eventID = eventItem.getProperty("event_id");
                OResultSet travEvent = database.query(queryTravEvent,eventID);

                while(travEvent.hasNext()){
                    OResult patItem = travEvent.next();

                    if(patItem.hasProperty("patient_mrn"){
                        if(patItem.hasProperty("patient_status")){
                            String patientStatus = patItem.getProperty("patient_status");

                            if(patientStatus.equals("in-patient")){
                                inPatientCount++;

                                if(patItem.hasProperty("vaccination_status") && patItem.getProperty("vaccination_status").equals("vaccinated")){
                                    vaccinatedInPatientCount++;
                                }
                            }else if(patientStatus.equals("icu-patient")){
                                icuPatientCount++;
                                if(patItem.hasProperty("vaccination_status") && patItem.getProperty("vaccination_status").equals("vaccinated")){
                                    vaccinatedIcuPatientCount++;
                                }
                            }else if(patientStatus.equals("patient-vent")){
                                ventPatientCount++;
                                if(patItem.hasPropety("vaccination_status") && patItem.getProperty("vaccination_status").equals(vaccinated)){
                                    vaccinatedVentPatientCount++;
                                }
                            }
                        }
                    }
                }
                shortMap.put(String.valueOf(i),shortList);
                contactList.add(shortMap);
            }
        }

        travPat.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        database.close();
        orient.close();

        double inPatientVaxPercentage = inPatientCount > 0 ? (double) vaccinatedInPatientCount / inPatientCount : 0;
        double icuPatientVaxPercentage = icuPatientCount > 0 ? (double) vaccinatedIcuPatientCount / icuPatientCount : 0;
        double patientVentVaxPercentage = patientVentCount > 0 ? (double) vaccinatedVentPatientCount / ventPatientCount : 0;


        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("hospital_id", hospital_id);
        jsonObject.addProperty("in_patient_count", inPatientCount);
        jsonObject.addProperty("icu_patient_count", icuPatientCount);
        jsonObject.addProperty("patient_vent_count", ventPatientCount);
        jsonObject.addProperty("vaccinated_in_patients", vaccinatedInPatientCount);
        jsonObject.addProperty("vaccinated_icu_patients", vaccinatedIcuPatientCount);
        jsonObject.addProperty("vaccinated_patients_on_vent", vaccinatedVentPatientCount);
        jsonObject.addProperty("in_patient_vax_percentage", inPatientVaxPercentage);
        jsonObject.addProperty("icu_patient_vax_percentage", icuPatientVaxPercentage);
        jsonObject.addProperty("patient_vent_vax_percentage", patientVentVaxPercentage);

        return jsonObject.toString();

        Gson gson = new Gson();
        return gson.toJson(contactList);
    }

}
