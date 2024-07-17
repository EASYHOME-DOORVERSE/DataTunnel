package com.dongwo.data.tunnel.canal.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : Leon on XXM Mac
 * @since : create in 2024/2/28 2:38 PM
 */
@Data
@Slf4j
@Component
@ConfigurationProperties("tunnel")
public class CanalTunnelConfig {

    /**
     * TCP 模式 配置
     */
    private Tcp tcp = new Tcp();

    /**
     * RocketMQ 模式 配置
     */
    private RocketMq rocketMq = new RocketMq();

    /**
     * 失败回调
     */
    private Fallback fallback = new Fallback();

    @Data
    public static class Tcp{

        /**
         * 服务端配置
         */
        private Server server = new Server();

        /**
         * 客户端配置
         */
        private Client client = new Client();


        @Data
        public static class Server{
            /**
             * Canal 服务端地址
             */
            private String host = "127.0.0.1";

            /**
             * Canal 服务端端口
             */
            private Integer port = 11111;

            /**
             * 订阅 canal 的 destination
             */
            private String destination = "example";

            /**
             * 订阅 canal 的 username
             */
            private String username;

            /**
             * 订阅 canal 的 password
             */
            private String password;
        }


        @Data
        public static class Client{

            /**
             * 订阅 canal 的 filter
             */
            private String filterRegex;
        }
    }


    @Data
    public static class RocketMq{

        /**
         * RocketMQ 服务端地址
         */
        private String nameServers;

        /**
         * RocketMQ 订阅主题
         */
        private String topic = "TP_canal_tunnel_brand_1616";

        /**
         * RocketMQ aliyun ASK
         */
        private String accessKey;

        /**
         * RocketMQ aliyun ASK
         */
        private String secretKey;

        /**
         * 是否开启message trace
         */
        private Boolean enableMessageTrace = false;

        /**
         * message trace的topic
         */
        private String customizedTraceTopic;

        /**
         * RocketMQ local/cloud
         */
        private String accessChannel = "local";

        /**
         * rocketmq实例id
         */
        private String namespace;

        /**
         * 是否开启扁平化消息
         */
        private Boolean flatMessage = false;

        /**
         * RocketMQ 消费者配置
         */
        private Consumer consumer = new Consumer();

        @Data
        public static class Consumer{

            /**
             * 消费者组
             */
            private String groupId = "S_canal_tunnel_brand_1616";
        }
    }


    @Data
    public static class Fallback {

        /**
         * 是否开启失败回调
         */
        private Boolean activate = false;

        /**
         * 告警配置
         */
        private Alarm alarm = new Alarm();

        @Data
        public static class Alarm {

            /**
             * 告警地址
             */
            private String webhook = "https://oapi.dingtalk.com/robot/send?access_token=47d0b54";

            /**
             * 告警处理程序
             */
            private Map<String, List<String>> processor = new HashMap<>();
        }
    }
}
