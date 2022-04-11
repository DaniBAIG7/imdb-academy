package co.empathy.academy.search.util;

import jakarta.json.JsonObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TsvReader {

    private BufferedReader r;
    private boolean open;

    public TsvReader() {
        try {
            r = new BufferedReader(new FileReader("src/main/resources/static/title.basics.tsv"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Map<String, JsonObject> readSeveral() {
        Map<String, JsonObject> toRet = new HashMap<>();
        try {
            JsonParser jParser = new JsonParser(this.r.readLine().split("\t"));

            for(int i = 0; i < 1000; i++) {
                String readline;
                if((readline = r.readLine()) != null) {
                    JsonObject job = jParser.of(readline);
                    toRet.put(String.valueOf(job.get("tconst")), job);
                } else {
                    this.r.close();
                    break;
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return toRet;
    }

    public void setOpen(boolean b) {
        this.open = b;
    }

    public boolean getOpen() {
        return this.open;
    }

}
