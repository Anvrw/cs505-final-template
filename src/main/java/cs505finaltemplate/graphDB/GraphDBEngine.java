
package cs505finaltemplate.graphDB;

import cs505finaltemplate.Topics.PatientData;


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

        db.close();
        orient.close();

    }

    //Only used in the /reset function as a part of the API
    public void resetDB(){
        OrientDB database = new OrientDB("remote:ajta238.cs.uky.edu", OrientDBConfig.defaultConfig());

        if(database.exists(databaseName)){
            database.drop(databaseName);
        }
        database.create(databaseName, ODatabaseType.PLOCAL);
        database.close();
    }

    //general case to add a patient
    public void addPatient(PatientData newPatient){
        
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
            PatientBuffer.setProperty("patient_status", newPatient.patient_status);
            PatientBuffer.setProperty("patient_zipcode", newPatient.patient_zipcode);
            PatientBuffer.setProperty("patient_status", newPatient.patient_status);
            PatientBuffer.setProperty("contact_list", newPatient.contact_list);
            PatientBuffer.setProperty("event_list", newPatient.event_list);
            PatientBuffer.setProperty("hospital_status",0);
            PatientBuffer.save();
        }

        else if (result.next().isVertex()){
            OVertex PatientBuffer = result.next().getVertex().get();
            PatientBuffer.setProperty("testing_id", newPatient.testing_id);
            PatientBuffer.setProperty("patient_mrn", newPatient.patient_mrn);
            PatientBuffer.setProperty("patient_name", newPatient.patient_name);
            PatientBuffer.setProperty("patient_status", newPatient.patient_status);
            PatientBuffer.setProperty("patient_zipcode", newPatient.patient_zipcode);
            PatientBuffer.setProperty("patient_status", newPatient.patient_status);
            PatientBuffer.setProperty("contact_list", newPatient.contact_list);
            PatientBuffer.setProperty("event_list", newPatient.event_list);
            PatientBuffer.setProperty("hospital_status",0);
            PatientBuffer.save();
        }
        
        orient.close();
    }

    //Incase of a random json string
    public void addPatient(String jsoString){
        Gson gson = new Gson();
        PatientData newPatient = gson.fromJson(jsoString, PatientData.class);
        addPatient(newPatient);
    }

    //Default, keep this way
    private OVertex createPatient(ODatabaseSession db, String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
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
        

    }

}
