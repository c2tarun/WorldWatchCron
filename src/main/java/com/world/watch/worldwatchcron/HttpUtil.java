/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.world.watch.worldwatchcron;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tarun
 */
public class HttpUtil {

    private static final String TAG = HttpUtil.class.getName();
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    /**
     * First parameter should be URL then after that key, value, key, value,
     * key, value
     *
     * @param input
     * @return
     */
    public static String getStringFromURL(String[] input) {
        InputStream in = getInputStream(input);
        if(in == null)
            return null;
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        StringBuilder sb = new StringBuilder();
        try {
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static String getStringFromURL(String inputUrl) {
        StringBuilder sb = new StringBuilder();
        try {
            URL url = new URL(inputUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            InputStream in = con.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static InputStream getInputStream(String[] input) {
        URL url;
        try {
            url = new URL(getEncodedUrl(input));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            return con.getInputStream();
        } catch (IOException e) {
            logger.debug("News fetch failed", e);
        }
        return null;
    }

    public static InputStream getInputStream(String baseUrl, HashMap<String, String> params) {
        try {
            URL url = new URL(getEncodedUrl(baseUrl, params));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            return con.getInputStream();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    private static String getEncodedUrl(String[] input) {
        String baseUrl = input[0];
        HashMap<String, String> urlParams = new HashMap<String, String>();
        for (int i = 1; i < input.length; i = i + 2) {
            urlParams.put(input[i], input[i + 1]);
        }
        return getEncodedUrl(baseUrl, urlParams);
    }

    private static String getEncodedUrl(String baseUrl, HashMap<String, String> urlParams) {
        String encodedUrl = baseUrl + "?" + getEncodedParams(urlParams);
        return encodedUrl;
    }

    private static String getEncodedParams(HashMap<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (String key : params.keySet()) {
            String value = params.get(key);
            if (sb.length() > 0) {
                sb.append("&");
            }
            sb.append(key + "=" + value);

        }
        return sb.toString();
    }

    public static String encodeString(String str) {
        try {
            return URLEncoder.encode(str, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(HttpUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    
    
}
