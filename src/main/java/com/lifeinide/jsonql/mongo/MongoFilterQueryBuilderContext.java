package com.lifeinide.jsonql.mongo;

import com.lifeinide.jsonql.core.BaseQueryBuilderContext;
import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * A context for {@link MongoFilterQueryBuilder}.
 *
 * @author Lukasz Frankowski
 */
public class MongoFilterQueryBuilderContext<E> extends BaseQueryBuilderContext {

	protected MongoCollection<E> collection;
	protected Stack<List<Bson>> filters = new Stack<>();

	public MongoFilterQueryBuilderContext(MongoCollection<E> collection) {
		this.collection = collection;
		this.filters.push(new ArrayList<>());
	}

	public MongoCollection<E> getCollection() {
		return collection;
	}

	public List<Bson> getFilters() {
		return filters.peek();
	}

	List<Bson> doWithNewFilters(Runnable r) {
		this.filters.push(new ArrayList<>());
		r.run();
		return this.filters.pop();
	}

}
