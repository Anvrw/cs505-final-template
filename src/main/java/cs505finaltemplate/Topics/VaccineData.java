package cs505finaltemplate.Topics;


public class VaccineData {
    public int vaccination_id;
    public String patient_name;
    public String patient_mrn;

    public VaccineData(String patient_name, String patient_mrn,
    int vaccination_id){
        this.patient_name = patient_name;
        this.patient_mrn = patient_mrn;
        this.vaccination_id = vaccination_id;
    }
}
