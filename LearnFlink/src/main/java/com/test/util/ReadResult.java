package com.test.util;

public class ReadResult {
    private final int responseCode;
    private final String responseBody;

    public ReadResult(int responseCode, String responseBody) {
        this.responseCode = responseCode;
        this.responseBody = responseBody;
    }

    public int getResponseCode() {
        return this.responseCode;
    }

    public String getResponseBody() {
        return this.responseBody;
    }


    public String toJsonString(){
        return responseBody;
    }

}
