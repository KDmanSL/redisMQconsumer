package com.mqconsumer.listener;

import com.baidu.aip.ocr.AipOcr;
import com.mqconsumer.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class SpringRabbitListener {
    @RabbitListener(queues = "baidu.ai.mq")
    public void listenSimpleQueue(Map msg){
        String img_info = imgHandlerAI(msg.get("file_url").toString());
        System.out.println("uuid: "+ msg.get("uuid"));
        System.out.println("img_info: "+ img_info);

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
}
