package com.muheda.notice.es.service.impl;

import com.muheda.notice.es.repository.DemoRepository;
import com.muheda.notice.es.service.DemoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description:
 * @author: zxl
 * @Data:2021/7/25
 */
@Service
public class DemoServiceImpl implements DemoService {
    @Autowired
    private DemoRepository demoRepository;
    public void test(){

    }
}
