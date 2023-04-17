package cs505finaltemplate.Topics;


public class HospPatData {
    public String patient_name;
    public String hospital_id;
    public String patient_mrn;
    public int patient_status;

    public HospPatData(String patient_name, String hospital_id, String patient_mrn,
    int patient_status){
        this.patient_name = patient_name;
        this.hospital_id = hospital_id;
        this.patient_mrn = patient_mrn;
        this.patient_status = patient_status;
    }
}
