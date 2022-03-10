package com.muheda.notice.controller;

import com.alibaba.fastjson.JSON;

import com.muheda.notice.es.entity.DemoES;
import com.muheda.notice.es.repository.DemoRepository;
import com.muheda.notice.hbase.HBaseUtils;
import com.muheda.notice.hbase.entity.Demo;
import com.muheda.notice.utils.BaseController;
import com.muheda.notice.utils.ResultData;
import com.muheda.notice.utils.ResultType;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

import com.muheda.notice.hbase.mapper.DemoMapper;

@RestController
@RequestMapping("/demo")
public class DemoController extends BaseController {
    @Autowired
    private DemoRepository demoRepository;
    @Autowired
    private com.muheda.notice.kafka.SpringKafkaProducer SpringKafkaProducer;
    @Autowired
    private DemoMapper demoMapper;

    @ApiOperation(value = "测试Es", notes = "测试Es")
    @GetMapping("/test")
    public ResultData test() {
        DemoES demo = new DemoES();
        demo.setAge("19");
        demo.setIdCart("34335443");
        demo.setRow("a68e50fe-47b7-4be0-acd1-9156420db051");
        demo.setName("林俊杰443");
        DemoES save = demoRepository.save(demo);
        System.err.println("我是测试" + save.toString());
        return setResponseEntity(ResultType.SUCCESS.getDescription(), ResultType.SUCCESS.getCode(), null, true);
    }

    @ApiOperation(value = "查询数据接口", notes = "查询数据接口")
    @GetMapping("/add")
    public ResultData add() throws InterruptedException {
        for (int k = 0; k < 6000; k++) {
            for (int i = 0; i < 500; i++) {
                String row = UUID.randomUUID().toString();
                Demo demo = Demo.builder().idCart("34335" + i).age("19").name("林俊杰" + i).build();
                boolean result = HBaseUtils.putRow("person", row, "idCart", "idCart", demo.getIdCart());
                boolean result2 = HBaseUtils.putRow("person", row, "age", "age", demo.getAge());
                boolean result3 = HBaseUtils.putRow("person", row, "name", "name", demo.getName());
                if (result && result2 && result3) {
                    Map<String, String> map = HBaseUtils.getData("person", row);
                    map.put("row", row);
                    String messege = JSON.toJSONString(map);
                    System.err.println("messege: " + messege);
                    SpringKafkaProducer.produce("SPRING_TEST_TOPIC", messege);
                }

            }
        }

        return setResponseEntity(ResultType.SUCCESS.getDescription(), ResultType.SUCCESS.getCode(), null, true);

    }

    @ApiOperation(value = "查询数据接口", notes = "查询数据接口")
    @GetMapping("/produce")
    public ResultData produce() {
        List<Map<String, String>> mapList = HBaseUtils.getData("person");
        System.err.println(mapList.toString());
        String messege = JSON.toJSONString(mapList.get(0));
        System.err.println("messege: " + messege);
        SpringKafkaProducer.produce("SPRING_TEST_TOPIC", messege);
        return setResponseEntity(ResultType.SUCCESS.getDescription(), ResultType.SUCCESS.getCode(), null, true);
    }

    @ApiOperation(value = "hbase查询数据接口", notes = "hbase查询数据接口")
    @GetMapping("/hbase/get")
    public ResultData produce(@RequestParam("row") String row) {
        Map<String, String> demo = HBaseUtils.getData("person", row);
        return setResponseEntity(ResultType.SUCCESS.getDescription(), ResultType.SUCCESS.getCode(), demo, true);
    }

    /**
     * 2、查  ++:全文检索（根据整个实体的所有属性，可能结果为0个）
     *
     * @param key
     * @return
     */
    @ApiOperation(value = "es搜索", notes = "es搜索")
    @GetMapping("/esByKey")
    public List<DemoES> testSearch(@RequestParam("key") String key) {
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder(key);
        Iterable<DemoES> searchResult = demoRepository.search(builder);
        Iterator<DemoES> iterator = searchResult.iterator();
        List<DemoES> list = new ArrayList<DemoES>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

    public static void main(String[] args) {
        Integer a = Integer.parseInt(null);
        System.out.println(a);
    }
}
