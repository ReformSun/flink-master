package com.test.alarm;

import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class AssignerRow implements AssignerElement<Row>{
	private final TypeInformation[] typeInformations;
	private volatile Row row;
	public AssignerRow(TypeInformation[] typeInformations) {
		this.typeInformations = typeInformations;
	}

	@Override
	public Row getElement() {
		return getRow();
	}


//	private Row getRow(){
//		if (row == null){
//			synchronized (AssignerRow.class){
//				if (row == null){
//					row = new Row(typeInformations.length);
//					int i = 0;
//					for (TypeInformation typeInformation:typeInformations){
//						row.setField(i,getObject(typeInformation));
//						i++;
//					}
//				}
//			}
//		}
//		return row;
//	}
	private Row getRow(){
		Row row = new Row(typeInformations.length);
		int i = 0;
		for (TypeInformation typeInformation:typeInformations){
			row.setField(i,getObject(typeInformation));
			i++;
		}
		return row;
	}

	private Object getObject(TypeInformation info){
		if (info.equals(Types.VOID)) {
			return null;
		} else if (info.equals(Types.BOOLEAN)) {
			return Boolean.valueOf(true);
		} else if (info.equals(Types.STRING) ) {
			return "aa";
		} else if (info.equals(Types.LONG)) {
			return Long.valueOf(1);
		}else if (info.equals(Types.DOUBLE)) {
			return Double.valueOf(1);
		}else if (info.equals(Types.FLOAT)) {
			return Float.valueOf(1);
		}else if (info.equals(Types.INT)) {
			return Integer.valueOf(1);
		}else if (info.equals(Types.CHAR)) {
			return Character.valueOf('a');
		}else if (info.equals(Types.SHORT)) {
			return Short.valueOf("a");
		}else if (info.equals(Types.BIG_DEC)) {
			return BigDecimal.valueOf(1L);
		} else if (info.equals(Types.BIG_INT)) {
			return BigInteger.valueOf(1L);
		} else if (info.equals(Types.SQL_DATE)) {
			return new Date(System.currentTimeMillis());
		} else if (info.equals(Types.SQL_TIME)) {
			return new Time(System.currentTimeMillis());
		} else if (info.equals(Types.SQL_TIMESTAMP)) {
			return new Timestamp(System.currentTimeMillis());
		}
		return null;
	}

	public static void main(String[] args) {
		AssignerRow assignerRow = new AssignerRow(new TypeInformation[1]);
		TypeInformation typeInformation = Types.STRING;
		System.out.println(assignerRow.getObject(typeInformation));
	}


}
