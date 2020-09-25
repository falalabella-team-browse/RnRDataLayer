package com.hachathon.reviewNratings.service;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class RedisService {

    @Value("${spring.redis.host}")
    private  String host;

    @Value("${spring.redis.port}")
    private  int port;

    public RedissonClient getRedissonClient(){
        RedissonClient client  = null;
        if(client == null) {
            Config config = new Config();
            String url = "redis://" + host +":"+port;
            config.useSingleServer()
                    .setAddress(url).setConnectionMinimumIdleSize(10);
            client = Redisson.create(config);
        }
        return client;
    }
}

