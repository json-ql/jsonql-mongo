package com.lifeinide.jsonql.mongo;

import com.lifeinide.jsonql.core.dto.Page;
import com.mongodb.client.MongoCollection;

/**
 * {@link MongoFilterQueryBuilder} with default {@link Page} results.
 *
 * @author Lukasz Frankowski
 */
public class DefaultMongoFilterQueryBuilder<E> extends MongoFilterQueryBuilder<E, Page<E>> {

	public DefaultMongoFilterQueryBuilder(MongoCollection<E> collection) {
		super(collection);
	}

}
