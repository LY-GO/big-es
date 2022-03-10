package com.muheda.notice.hbase.entity;

import com.muheda.notice.hbase.HbaseColumn;
import com.muheda.notice.hbase.HbaseTable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Sorin
 * @Descriptions:
 * @Date: Created in 2018/3/22
 */
@HbaseTable(tableName="person")
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Demo {

	@HbaseColumn(family="rowkey", qualifier="idCart")
	private String idCart;

	@HbaseColumn(family="demo", qualifier="name")
	private String name;

	@HbaseColumn(family="demo", qualifier="age")
	private String age;
	private String row;

}
