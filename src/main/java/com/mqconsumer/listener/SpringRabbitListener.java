package com.mqconsumer.listener;

import com.baidu.aip.ocr.AipOcr;
import com.mqconsumer.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class SpringRabbitListener {
    @RabbitListener(queues = "baidu.ai.mq")
    public void listenSimpleQueue(Map msg){
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            log.error("线程休眠异常，{}", e.getMessage());
        }
        String img_info = imgHandlerAI(msg.get("file_url").toString());
        String uuid = msg.get("uuid").toString();

        log.info("uuid:{}, img_info:{}", uuid, img_info);

        // 结果保存到本地
        saveToCsv(uuid, img_info);

    }

    public String imgHandlerAI(String imgurl) {
        // 处理图片比特流，调用百度AI,识别图片，返回图片信息
        AipOcr client = new AipOcr(SystemConstants.APP_ID
                , SystemConstants.API_KEY, SystemConstants.SECRET_KEY);
        HashMap<String, String> options = new HashMap<String, String>(4);
        options.put("detect_direction", "true");
        options.put("detect_language", "false");
        options.put("probability", "false");


        JSONObject res = client.basicGeneralUrl(imgurl, options);
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
}
