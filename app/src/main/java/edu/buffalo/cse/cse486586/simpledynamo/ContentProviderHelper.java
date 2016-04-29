package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.net.Uri;

/**
 * Created by mithun on 4/26/16.
 */
public class ContentProviderHelper {

    String scheme = "content";
    String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";

    protected Uri buildUri() {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    protected ContentValues buildContentValues(String filename, String receivedMsg){
        ContentValues cv = new ContentValues();
        cv.put("key", filename);
        cv.put("value", receivedMsg);

        return cv;
    }
}
