package com.mqconsumer.listener;

import com.baidu.aip.ocr.AipOcr;
import com.mqconsumer.utils.SystemConstants;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class SpringRedisStreamListener {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Value("${web.upload-path}")
    private String uploadPath;
    public static final String MQ_NAME_SERVER = "stream.mq.server";
    @PostConstruct
    public void listenRedisStreamQueue(){
        while (true) {
            try {
                // 从消息队列获取消息
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                        StreamOffset.create(MQ_NAME_SERVER, ReadOffset.lastConsumed())
                );
                if (list == null || list.isEmpty()) {
                    continue;
                }
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                // 获取消息
                String img_url = (String) value.get("file_url");
                String uuid = (String) value.get("uuid");

                String img_local_url = downloadImage(img_url);// 保存图片到本地

                // 将img_bit调用
                String img_info = imgHandlerAI(img_local_url);
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    log.error("线程休眠异常，{}", e.getMessage());
                }
                // 确认消息ACK
                stringRedisTemplate.opsForStream().acknowledge(MQ_NAME_SERVER, "g1", record.getId().getValue());

                log.info("uuid:{}, img_info:{}", uuid, img_info);

                // 结果保存到本地
                saveToCsv(uuid, img_info);

            } catch (Exception e) {
                log.error("消息处理异常，{}", e.getMessage());
                // 当出现异常时，重新尝试处理pending-list中的消息
                handlePendingList();
            }
        }
    }
    private void handlePendingList() {
        while (true) {
            try {
                // 从消息队列获取消息
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(MQ_NAME_SERVER, ReadOffset.from("0"))
                );
                if (list == null || list.isEmpty()) {
                    // 说明异常消息已解决
                    break;
                }
                for (MapRecord<String, Object, Object> record : list) {
                    Map<Object, Object> value = record.getValue();
                    // 获取消息
                    String img_url = (String) value.get("file_url");
                    String uuid = (String) value.get("uuid");

                    String img_local_url = downloadImage(img_url);// 保存图片到本地

                    // 将img_bit调用
                    String img_info = imgHandlerAI(img_local_url);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        log.error("线程休眠异常，{}", e.getMessage());
                    }
                    // 确认消息ACK
                    stringRedisTemplate.opsForStream().acknowledge(MQ_NAME_SERVER, "g1", record.getId().getValue());


                    // 结果保存到本地
                    saveToCsv(uuid, img_info);
                }
            } catch (Exception e) {
                log.error("消息处理异常，{}", e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }
    public String imgHandlerAI(String imgurl) {
        // 处理图片比特流，调用百度AI,识别图片，返回图片信息
        AipOcr client = new AipOcr(SystemConstants.APP_ID
                , SystemConstants.API_KEY, SystemConstants.SECRET_KEY);
        HashMap<String, String> options = new HashMap<String, String>(4);
        options.put("detect_direction", "true");
        options.put("detect_language", "false");
        options.put("probability", "false");


        JSONObject res = client.basicGeneral(imgurl, options);
        String jsonData = "";
        try {
            jsonData = res.toString(2);
        } catch (JSONException e) {
            log.error("获取json数据异常，{}", e.getMessage());
        }
        return jsonData;
    }
    private void saveToCsv(String uuid, String imgInfo) {
        boolean isFileNew = false;
        try (FileOutputStream fos = new FileOutputStream("output.csv", true);
             Writer writer = new OutputStreamWriter(fos, "UTF-8");
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            if (new File("output.csv").length() == 0) {
                csvPrinter.printRecord("UUID", "Image Info");
                isFileNew = true;
            }
            csvPrinter.printRecord(uuid, imgInfo);
            csvPrinter.flush();
        } catch (IOException e) {
            log.error("写入CSV文件时发生错误: {}", e.getMessage());
        }
    }
    public String downloadImage(String imageUrl) throws IOException {
        URL url = new URL(imageUrl);
        InputStream in = url.openStream();
        String fileName = uploadPath + imageUrl.substring(imageUrl.lastIndexOf("/") + 1);

        File directory = new File(uploadPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        try (FileOutputStream out = new FileOutputStream(fileName)) {
            StreamUtils.copy(in, out);
        } finally {
            in.close();
        }
        return fileName;
    }
}
