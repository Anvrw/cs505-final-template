package cs505finaltemplate.Topics;

public class HospitalData {
    public int id;
    public String name;
    public String address;
    public String city;
    public String state;
    public int zipcode;
    public String type;
    public int beds;
    public String county;
    public int countyfips;
    public String country;
    public int latitude;
    public int longitude;
    public int naics_code;
    public String website;
    public String owner;
    public String trauma;
    public String helipad;

    public HospitalData(int id,String name,String address,String city,String state,
     int zipcode,String type,int beds,String county,int countyfips,String country,
     int latitude,int longitude,int naics_code,String website,String owner,String trauma,
     String helipad){

        this.id = id;
        this.name = name;
        this.address = address;
        this.city = city;
        this.state = state;
        this.zipcode = zipcode;
        this.type = type;
        this.beds = beds;
        this.county = county;
        this.countyfips = countyfips;
        this.country = country;
        this.latitude = latitude;
        this.longitude = longitude;
        this.naics_code = naics_code;
        this.website = website;
        this.owner = owner;
        this.trauma = trauma;
        this.helipad = helipad;

    }
}
