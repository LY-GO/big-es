package com.muheda.notice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Author: Sorin
 * @Descriptions:
 * @Date: Created in 2018/3/21
 */
@SpringBootApplication
@MapperScan(basePackages = {"com.muheda.notice.hbase.mapper"})
public class NoticeStartApp {

    public static void main(String[] args) {
        SpringApplication.run(NoticeStartApp.class, args);
    }
}
