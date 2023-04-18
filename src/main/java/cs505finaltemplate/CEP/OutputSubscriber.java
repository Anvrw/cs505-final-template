package cs505finaltemplate.CEP;

import com.jayway.jsonpath.internal.function.text.Length;

import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;


public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            System.out.println("");

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            String[] oldTokens = Launcher.lastCEPOutput.replaceAll("[^0-9,]", "").split(",");
            String[] msgTokens = String.valueOf(msg).replaceAll("[^0-9,]", "").split(",");

            
            for(int i=0; i<msgTokens.length;i+=2){
                for(int j=0; j<oldTokens.length;j+=2){
                    if(msgTokens[i].equals(oldTokens[j])){
                        int countNew = Integer.parseInt(msgTokens[j+1]);
                        int countOld = Integer.parseInt(oldTokens[j+1]);

                        if(countNew < 2*countOld){
                            Launcher.alertlist.remove(oldTokens[i]);  
                        }
                        else {
                            if(Launcher.alertlist.indexOf(msgTokens[i])!=-1)
                                Launcher.alertlist.add(oldTokens[i]);
                        }
                        break;
                    }
                }
            }

            Launcher.lastCEPOutput = String.valueOf(msg);
        } 
        catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
