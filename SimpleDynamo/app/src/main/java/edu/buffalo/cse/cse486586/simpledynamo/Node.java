package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

/**
 * Created by Haril Satra on 5/6/2017.
 */

public class Node implements Serializable {
    String info;
    String port;
    String key;
    String value;
    String msg;
    String hash;
    String query;
    String query_result;
    String del;
}
