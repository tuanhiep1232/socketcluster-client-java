package io.github.sac;


import com.fasterxml.jackson.databind.JsonNode;

/**
 * Created by sachin on 15/11/16.
 */
public class Parser {

    public enum ParseResult {
        ISAUTHENTICATED,
        PUBLISH,
        REMOVETOKEN,
        SETTOKEN,
        EVENT,
        ACKRECEIVE
    }


    public static ParseResult parse(JsonNode dataobject, String event) {

        if (dataobject.has("isAuthenticated") && !dataobject.get("isAuthenticated").isNull()) {
            return ParseResult.ISAUTHENTICATED;
        } else if (event != null) {
            if (event.equals("#publish")) {
                return ParseResult.PUBLISH;
            } else if (event.equals("#removeAuthToken")) {
                return ParseResult.REMOVETOKEN;
            } else if (event.equals("#setAuthToken")) {
                return ParseResult.SETTOKEN;
            } else {
                return ParseResult.EVENT;
            }
        } else {
            return ParseResult.ACKRECEIVE;
        }
    }

}
