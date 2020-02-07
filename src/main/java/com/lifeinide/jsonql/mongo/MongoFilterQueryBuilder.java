package com.lifeinide.jsonql.mongo;

import com.lifeinide.jsonql.core.BaseFilterQueryBuilder;
import com.lifeinide.jsonql.core.dto.BasePageableRequest;
import com.lifeinide.jsonql.core.dto.Page;
import com.lifeinide.jsonql.core.filters.*;
import com.lifeinide.jsonql.core.intr.FilterQueryBuilder;
import com.lifeinide.jsonql.core.intr.Pageable;
import com.lifeinide.jsonql.core.intr.QueryFilter;
import com.lifeinide.jsonql.core.intr.Sortable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementation of {@link FilterQueryBuilder} with {@link MongoCollection}.
 *
 * @author Lukasz Frankowski
 */
public class MongoFilterQueryBuilder<E, P extends Page<E>>
extends BaseFilterQueryBuilder<E, P, Bson, MongoFilterQueryBuilderContext<E>, MongoFilterQueryBuilder<E, P>> {

	public static final Bson EMPTY_BSON = new Document();

	protected MongoFilterQueryBuilderContext<E> context;

	public MongoFilterQueryBuilder(MongoFilterQueryBuilderContext<E> context) {
		this.context = context;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilderContext<E> context() {
		return context;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable DateRangeQueryFilter filter) {
		// TODOLF impl MongoFilterQueryBuilder.add
		return null;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable EntityQueryFilter<?> filter) {
		// TODOLF impl MongoFilterQueryBuilder.add
		return null;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable ListQueryFilter<? extends QueryFilter> filter) {
		// TODOLF impl MongoFilterQueryBuilder.add
		return null;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable SingleValueQueryFilter<?> filter) {
		// TODOLF impl MongoFilterQueryBuilder.add
		return null;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable ValueRangeQueryFilter<? extends Number> filter) {
		// TODOLF impl MongoFilterQueryBuilder.add
		return null;
	}

	@Nonnull
	@Override
	public Bson build(@Nonnull Pageable pageable, @Nonnull Sortable<?> sortable) {
		List<Bson> filters = context.getFilters();

		if (filters.isEmpty())
			return EMPTY_BSON;
		else if (filters.size()==1)
			return filters.iterator().next();
		else
			return Filters.and(filters);
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> or(@Nonnull Runnable r) {
		return merge(context.doWithNewFilters(r), Filters::or);
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> and(@Nonnull Runnable r) {
		return merge(context.doWithNewFilters(r), Filters::and);
	}

	protected MongoFilterQueryBuilder<E, P> merge(@Nonnull List<Bson> filters, @Nonnull Function<List<Bson>, Bson> f) {
		if (!filters.isEmpty()) {
			if (filters.size()==1)
				context.getFilters().add(filters.iterator().next());
			else
				context.getFilters().add(f.apply(filters));
		}

		return this;
	}

	@SuppressWarnings({"ConstantConditions", "unchecked"})
	@Nonnull
	@Override
	public P list(@Nullable Pageable pageable, @Nullable Sortable<?> sortable) {
		if (pageable==null)
			pageable = BasePageableRequest.ofUnpaged();
		if (sortable==null)
			sortable = BasePageableRequest.ofUnpaged();

		Bson filter = build(pageable, sortable);
		if (filter.equals(EMPTY_BSON))
			filter = null;

		long count = context.getCollection().countDocuments(filter);
		FindIterable<E> iterable = context.getCollection().find(filter);

		if (pageable.isPaged()) {
			iterable.skip(pageable.getOffset());
			iterable.limit(pageable.getPageSize());
		}

		List<Bson> sorts = sortable.getSort().stream()
			.map(sort -> sort.isDesc() ? Sorts.descending(sort.getSortField()) : Sorts.ascending(sort.getSortField()))
			.collect(Collectors.toList());
		if (!sorts.isEmpty())
			iterable.sort(Sorts.orderBy(sorts));

		return (P) buildPageableResult(pageable.getPageSize(), pageable.getPage(), count, iterable.into(new ArrayList<>()));
	}

}
