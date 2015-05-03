/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.world.watch.worldwatchcron.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;
import json.model.worldwatch.PushData;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tarun
 */
public class PushToUser {

    protected static final String URL = "https://api.parse.com/1/push";
    private static final Logger logger = LoggerFactory.getLogger(PushToUser.class);

    public static void push(List<String> data, String userId) {
        try {
            InputStream jsonStream = PushToUser.class.getResourceAsStream("/parsePush.json");
            ObjectMapper mapper = new ObjectMapper();
            PushData push = mapper.readValue(jsonStream, PushData.class);
            push.getWhere().setUsername(userId);
            push.getData().setKeywords(data);
            String json = mapper.writeValueAsString(push);

            HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().build()).build();
            HttpPost post = new HttpPost(URL);
            post.setHeader("X-Parse-Application-Id", "WhqWj009luOxOtIH3rM9iWJICLdf0NKbgqdaui8Q");
            post.setHeader("X-Parse-REST-API-Key", "lThhKObAz1Tkt092Cl1HeZv4KLUsdATvscOaGN2y");
            post.setHeader("Content-Type", "application/json");
            logger.debug("JSON to push {}", json.toString());
            StringEntity strEntity = new StringEntity(json);
            post.setEntity(strEntity);
            httpClient.execute(post);
            logger.debug("Pushed {} to userId {}", data.toString(), userId);
        } catch (Exception ex) {
            logger.error("Push Failed for {} ", userId, ex);
        }

    }

}
