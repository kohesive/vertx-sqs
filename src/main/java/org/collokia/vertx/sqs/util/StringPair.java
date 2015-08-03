package org.collokia.vertx.sqs.util;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject
public class StringPair {

    private String first;
    private String second;

    public StringPair() {
    }

    public StringPair(StringPair that) {
        this.first = that.getFirst();
        this.second = that.getSecond();
    }

    public StringPair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public StringPair(JsonObject jsonObject) {
        this.first = jsonObject.getString("first");
        this.second = jsonObject.getString("second");
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(String second) {
        this.second = second;
    }

}
