package com.muheda.notice.es.entity;

import com.muheda.notice.hbase.HbaseColumn;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

/**
 * @Description:
 * @author: zxl
 * @Data:2021/7/25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
//index命名小写字母
@Document(indexName = "test4")
public class DemoES {
    @Id
    private String id;
    private String idCart;
    @Field(type = FieldType.Text, store = true)
    private String name;
    @Field(type = FieldType.Text, store = true)
    private String age;
    @Field(type = FieldType.Text, store = true)
    private String row;
}
