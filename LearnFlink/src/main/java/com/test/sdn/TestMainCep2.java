package com.test.sdn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class TestMainCep2 {
	public static void main(String[] args) {

	}

	/**
	 * 业务场景
	 * 第一条数据失败，第二条数据在一定得时间内成功 不报警 也就是返回false
	 * 第一条数据失败，第二条数据在一定时间内还是失败 报警 也就是返回true
	 */
	public static void testMethod1(){
		Pattern<Tuple2<Long,Boolean>,Tuple2<Long,Boolean>> pattern = Pattern.<Tuple2<Long,Boolean>>begin("first")
			.where(new SimpleCondition<Tuple2<Long, Boolean>>() {
				@Override
				public boolean filter(Tuple2<Long, Boolean> value) throws Exception {
					return false;
				}
			});



		
	}
	

}
