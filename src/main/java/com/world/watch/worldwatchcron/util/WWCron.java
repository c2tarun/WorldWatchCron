/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.world.watch.worldwatchcron.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.world.watch.worldwatchcron.HttpUtil;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.parse4j.Parse;
import org.parse4j.ParseException;
import org.parse4j.ParseObject;
import org.parse4j.ParseQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author tarun
 */
public class WWCron {

    //private static final Logger logger = LoggerFactory.getLogger(WWCron.class);
    private static final String SEARCH_HISTORY_TB = "SEARCH_HISTORY";
    private static final String USER_ID = "USER_ID";
    private static final String SEARCH_TEXT = "SEARCH_TEXT";
    private static final String NEWS_JSON_TB = "NEWS_JSON";
    protected static final String MAX_TS = "MAX_TS";
    protected static final String JSON = "JSON";
    public List<NewsData> latestNews;
    private static final Logger logger = LoggerFactory.getLogger(WWCron.class);

    public WWCron() {
        Parse.initialize("WhqWj009luOxOtIH3rM9iWJICLdf0NKbgqdaui8Q", "lThhKObAz1Tkt092Cl1HeZv4KLUsdATvscOaGN2y");
        latestNews = new ArrayList<>();
    }

    public static void main(String[] args) {
        try {
            WWCron cron = new WWCron();
            Map<String, Long> searchKeywords = cron.fetchKeywords();
            cron.populateLatestNews(searchKeywords);
            logger.debug("Latest news populated");
            cron.updateLatestNewsInParse();
            logger.debug("News updated in parse {}", StringUtils.join(cron.latestNews, ","));
            cron.sendPushToUsers();
            logger.debug("Pushed to users");
        } catch (ParseException ex) {
            logger.error("Parse Exception ", ex);
        }
    }

    private void populateLatestNews(Map<String, Long> searchKeywords) {
        for (String key : searchKeywords.keySet()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                logger.debug("Thread interrupted", ex);
            }
            String newsJson = fetchNewsJson(key);
            if (newsJson == null) {
                continue;
            }
            long latestTS = extractLatestTimeStamp(newsJson);
            long oldTS = searchKeywords.get(key);
            if (latestTS > oldTS) {
                NewsData news = new NewsData();
                news.latestTS = latestTS;
                news.newsJson = newsJson;
                news.searchText = key;
                latestNews.add(news);
            }
        }
    }

    public void updateLatestNewsInParse() throws ParseException {
        for (NewsData data : latestNews) {
            ParseQuery<ParseObject> query = ParseQuery.getQuery(NEWS_JSON_TB);
            query.whereEqualTo(SEARCH_TEXT, data.searchText);
            List<ParseObject> searchList = query.find();
            ParseObject news = searchList.get(0);
            news.put(MAX_TS, data.latestTS);
            news.put(JSON, data.newsJson);
            news.save();
        }
    }

    public void sendPushToUsers() throws ParseException {
        ParseQuery<ParseObject> query = ParseQuery.getQuery(SEARCH_HISTORY_TB);
        List<ParseObject> searches = query.find();
        for (ParseObject search : searches) {
            List<String> pushList = new ArrayList<>();
            String userId = search.getString(USER_ID);
            String sText = search.getString(SEARCH_TEXT);
            String[] searchTexts = sText.split(";");
            for (String searchText : searchTexts) {
                for (NewsData news : latestNews) {
                    if (news.searchText.equalsIgnoreCase(searchText)) {
                        pushList.add(searchText);
                        break;
                    }
                }
            }
            if (!pushList.isEmpty()) {
                logger.debug("Push list for {} is {}", userId, StringUtils.join(pushList, ","));
                PushToUser.push(pushList, userId);
            }
        }
    }

    public Map<String, Long> fetchKeywords() throws ParseException {
        ParseQuery<ParseObject> query = ParseQuery.getQuery(NEWS_JSON_TB);
        query.selectKeys(Arrays.asList(SEARCH_TEXT, MAX_TS));
        List<ParseObject> searchPObj = query.find();
        Map<String, Long> hashMap = new HashMap<>();
        for (ParseObject obj : searchPObj) {
            String searchText = obj.getString(SEARCH_TEXT);
            long timeStamp = obj.getLong(MAX_TS);
            hashMap.put(searchText, timeStamp);
        }
        return hashMap;
    }

    public String fetchNewsJson(String searchText) {
        try {
            searchText = URLEncoder.encode(searchText, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.debug("search text encoding failed");
        }
        String[] params = {"http://www.faroo.com/api", "q", searchText, "length", "10", "l",
            "en", "src", "news", "i", "true", "f", "json", "key", "FuZMJiD@y11PASdasEzBHKIYn2Q_"};
        String newsJson = HttpUtil.getStringFromURL(params);
        return newsJson;
    }

    public long extractLatestTimeStamp(String newsJson) {
        long maxTS = 0;
        JsonFactory factory = new JsonFactory();
        try {
            JsonParser parser = factory.createParser(newsJson);
            while (!parser.isClosed()) {
                JsonToken token = parser.nextToken();
                if (token == null) {
                    break;
                }
                String fieldName = parser.getCurrentName();
                if (fieldName != null && fieldName.equals("date")) {
                    parser.nextToken();
                    long date = Long.parseLong(parser.getText());
                    if (maxTS < date) {
                        maxTS = date;
                    }
                }
            }
        } catch (JsonParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            //logger.error("File not found ", e);
        }
        return maxTS;
    }

    public static class NewsData {

        public String searchText;
        public long latestTS;
        public String newsJson;
    }
}
