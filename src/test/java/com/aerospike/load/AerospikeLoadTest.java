package com.aerospike.load;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import junit.framework.TestCase;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;

public class AerospikeLoadTest{

    AerospikeClient client;
    String host="172.25.41.21";
    String port="3000";
    String testSchemaFile = "src/test/resources/testSchema.json";
    JSONObject testSchema = null;
    String ns = "test";
    String error_count = "0";
    String write_action = "update";
    String rootDir = "src/test/resources/";

    @Before
    public void setUp() throws Exception {
        try {
            client = new AerospikeClient(host, Integer.parseInt(port));
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (AerospikeException e) {
            e.printStackTrace();
        }
      //  testSchema = parseConfigFile(testSchemaFile);
    }

    @Test
    public void sampleLoad() throws  Exception{
        String dataFile = rootDir + "dataString.dsv";
        System.out.println("dataFile "+dataFile);
        AerospikeLoad.main(new String[]{"-h", host,"-p", port,"-n", ns, "-ec", error_count,"-wa", write_action,"-c", "src/test/resources/configString.json", dataFile});

        Assert.assertTrue(true);
    }

    private JSONObject parseConfigFile(String configFile) {
        JSONParser parser = new JSONParser();
        JSONObject jsonObject = null;
        try{
            Object obj = parser.parse(new FileReader(configFile));
            jsonObject = (JSONObject) obj;
        } catch (IOException e) {
            // Print error/abort/skip
        } catch (ParseException e) {
            // throw error/abort test/skip/test
        }
        return jsonObject;
    }
}



