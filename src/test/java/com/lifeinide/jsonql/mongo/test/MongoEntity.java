package com.lifeinide.jsonql.mongo.test;

import com.lifeinide.jsonql.core.test.IJsonQLTestEntity;
import com.lifeinide.jsonql.core.test.JsonQLTestEntityEnum;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * @author Lukasz Frankowski
 */
public class MongoEntity implements IJsonQLTestEntity<ObjectId> {

	@BsonId private ObjectId id;

	protected String stringVal;

	protected boolean booleanVal;

	protected Long longVal;

	protected BigDecimal decimalVal;

	protected LocalDate dateVal;

	protected JsonQLTestEntityEnum enumVal;

	@Override
	public ObjectId getId() {
		return id;
	}

	@Override
	public void setId(ObjectId id) {
		this.id = id;
	}

	@Override
	public String getStringVal() {
		return stringVal;
	}

	@Override
	public void setStringVal(String stringVal) {
		this.stringVal = stringVal;
	}

	@Override
	public boolean isBooleanVal() {
		return booleanVal;
	}

	@Override
	public void setBooleanVal(boolean booleanVal) {
		this.booleanVal = booleanVal;
	}

	@Override
	public Long getLongVal() {
		return longVal;
	}

	@Override
	public void setLongVal(Long longVal) {
		this.longVal = longVal;
	}

	@Override
	public BigDecimal getDecimalVal() {
		return decimalVal;
	}

	@Override
	public void setDecimalVal(BigDecimal decimalVal) {
		this.decimalVal = decimalVal;
	}

	@Override
	public LocalDate getDateVal() {
		return dateVal;
	}

	@Override
	public void setDateVal(LocalDate dateVal) {
		this.dateVal = dateVal;
	}

	@Override
	public JsonQLTestEntityEnum getEnumVal() {
		return enumVal;
	}

	@Override
	public void setEnumVal(JsonQLTestEntityEnum enumVal) {
		this.enumVal = enumVal;
	}
	
}
