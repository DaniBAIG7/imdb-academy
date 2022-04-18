package co.empathy.academy.search.util;

import jakarta.json.JsonObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TsvReader {

    private static final int BATCH_OPS = 10000;
    private BufferedReader r;
    private boolean open;
    private JsonParser jParser;

    /**
     * The constructor of this class creates the whole Adapter structure.
     * It creates a BufferedReader (the adaptee) and sets the reader to Open status.
     */
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

    /**
     * This method of the Reader does a limited reading of X documents (specified as a final parameter in the class).
     * The current value for reading is 10000 lines. This is done for building Bulk Operations later.
     *
     * readSeveral() is conceived to be called several times; So whenever TSVReader reads a null line, TSVReader is
     * set to Closed.
     *
     * @return a Map with String keys (IDs of the documents) and with JsonObject values (the values to index)
     */
    public Map<String, JsonObject> readSeveral() {
        Map<String, JsonObject> toRet = new HashMap<>();
        try {

            for(int i = 0; i < BATCH_OPS; i++) {
                String readline;
                if((readline = r.readLine()) != null) {
                    toRet.putAll(jParser.of(readline));
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
