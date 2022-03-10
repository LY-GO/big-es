package com.muheda.notice.hbase.mapper;



import com.muheda.notice.hbase.entity.Demo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface DemoMapper {

//    @Select("SELECT * from t_demo")
//    List<Demo> queryAll();
//
//    @Insert("UPSERT INTO t_demo VALUES( #{state}, #{city}, #{population} )")
//    void save(Demo USPopulation);

    @Select("SELECT * FROM t_demo WHERE id=#{state}")
    Demo queryById(String id);

//
//    @Delete("DELETE FROM t_demo WHERE state=#{state} AND city = #{city}")
//    void deleteByStateAndCity(String state, String city);

}
