package informatik.IAMTModelling;

import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import com.google.gson.annotations.SerializedName;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)

public class SensorData implements Serializable{

    public SensorData() {
        // Empty constructor required for deserialization
    }
    
    @SerializedName("component")
    private String component;
    @SerializedName("id")
    private String id;
    @SerializedName("temperature")
    private Double temperature;

    

    public String getComponent() {
        return component;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}



