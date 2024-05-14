package com.mqconsumer;

import com.mqconsumer.listener.SpringRabbitListener;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class MQconsumerApplicationTests {
    @Resource
    SpringRabbitListener springRabbitListener;
    @Test
    void contextLoads() {

        String imginfo=springRabbitListener.imgHandlerAI("http://119.3.158.9:8080/images/90ca85e642a04fd8a90b88eff3490234.jpg");
        System.out.println(imginfo);
    }

}
