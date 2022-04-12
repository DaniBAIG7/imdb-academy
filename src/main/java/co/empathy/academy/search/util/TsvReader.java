package co.empathy.academy.search.util;

import jakarta.json.JsonObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TsvReader {

    private final int BATCH_OPS = 1500;
    private BufferedReader r;
    private boolean open;
    private JsonParser jParser;

    public TsvReader() {
        try {
            r = new BufferedReader(new FileReader("src/main/resources/static/title.basics.tsv"));
            setOpen(true);
            try {
                this.jParser = new JsonParser(this.r.readLine().split("\t"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Map<String, JsonObject> readSeveral() {
        Map<String, JsonObject> toRet = new HashMap<>();
        try {

            for(int i = 0; i < BATCH_OPS; i++) {
                String readline;
                if((readline = r.readLine()) != null) {
                    JsonObject job = jParser.of(readline);
                    toRet.put(String.valueOf(job.get("tconst")), job);
                } else {
                    this.r.close();
                    setOpen(false);
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

    public boolean isOpen() {
        return this.open;
    }

}
