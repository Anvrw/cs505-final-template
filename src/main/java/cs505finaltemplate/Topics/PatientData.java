package cs505finaltemplate.Topics;

import java.util.List;

public class PatientData {

    public int testing_id;
    public String patient_name;
    public String patient_mrn;
    public int patient_zipcode;
    public int patient_status;
    public List<String> contact_list;
    public List<String> event_list;

    public  PatientData(int testing_id, int patient_zipcode, int patient_status, 
    String patient_name, String patient_mrn, 
    List<String> contact_list, List<String> event_list) {

        this.testing_id = testing_id;
        this.patient_name = patient_name;
        this.patient_mrn = patient_mrn;
        this.patient_zipcode = patient_zipcode;
        this.patient_status = patient_status;
        this.contact_list = contact_list;
        this.event_list = event_list;

    }

}