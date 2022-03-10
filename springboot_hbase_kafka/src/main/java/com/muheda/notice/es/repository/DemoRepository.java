package com.muheda.notice.es.repository;

import com.muheda.notice.es.entity.DemoES;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

/**
 * @Description:
 * @author: zxl
 * @Data:2021/7/25
 */

@Repository
public interface DemoRepository extends ElasticsearchRepository<DemoES, String> {

}
