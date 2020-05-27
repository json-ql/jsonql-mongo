package com.lifeinide.jsonql.mongo;

import com.lifeinide.jsonql.core.BaseQueryBuilderContext;
import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * A context for {@link MongoFilterQueryBuilder}.
 *
 * @author Lukasz Frankowski
 */
public class MongoFilterQueryBuilderContext<E> extends BaseQueryBuilderContext {

	@Nonnull protected MongoCollection<E> collection;
	@Nonnull protected Stack<List<Bson>> filters = new Stack<>();
	@Nonnull protected List<Bson> sorts = new ArrayList<>(); // allows to define default sorts if Sortable.sorts are not provided in a request

	public MongoFilterQueryBuilderContext(@Nonnull MongoCollection<E> collection) {
		this.collection = collection;
		this.filters.push(new ArrayList<>());
	}

	@Nonnull public MongoCollection<E> getCollection() {
		return collection;
	}

	@Nonnull public List<Bson> getFilters() {
		return filters.peek();
	}

	@Nonnull public List<Bson> getSorts() {
		return sorts;
	}

	List<Bson> doWithNewFilters(Runnable r) {
		this.filters.push(new ArrayList<>());
		r.run();
		return this.filters.pop();
	}

}
