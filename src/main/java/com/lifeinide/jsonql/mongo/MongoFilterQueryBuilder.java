package com.lifeinide.jsonql.mongo;

import com.lifeinide.jsonql.core.BaseFilterQueryBuilder;
import com.lifeinide.jsonql.core.dto.BasePageableRequest;
import com.lifeinide.jsonql.core.dto.Page;
import com.lifeinide.jsonql.core.enums.QueryConjunction;
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
import java.time.LocalDate;
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
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable SingleValueQueryFilter<?> filter) {
		if (filter!=null) {

			switch (filter.getCondition()) {

				case eq:
					context.getFilters().add(Filters.eq(field, filter.getValue()));
					break;

				case ne:
					context.getFilters().add(Filters.ne(field, filter.getValue()));
					break;

				case gt:
					context.getFilters().add(Filters.gt(field, filter.getValue()));
					break;

				case ge:
					context.getFilters().add(Filters.gte(field, filter.getValue()));
					break;

				case lt:
					context.getFilters().add(Filters.lt(field, filter.getValue()));
					break;

				case le:
					context.getFilters().add(Filters.lte(field, filter.getValue()));
					break;

				case isNull:
					context.getFilters().add(Filters.eq(field, null));
					break;

				case notNull:
					context.getFilters().add(Filters.not(Filters.eq(field, null)));
					break;

				default:
					throw new IllegalStateException(String.format("Not implemented: %s", filter.getCondition()));
					
			}

		}

		return this;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable ValueRangeQueryFilter<? extends Number> filter) {
		if (filter!=null) {
			Number from = filter.getFrom();
			Number to = filter.getTo();

			Bson fromFilter = from==null ? null : Filters.gte(field, from);
			Bson toFilter = to==null ? null : Filters.lte(field, to);

			fromFilter = (fromFilter!=null && toFilter!=null)
				? Filters.and(fromFilter, toFilter)
				: fromFilter != null ? fromFilter : toFilter;

			if (fromFilter!=null)
				context.getFilters().add(fromFilter);
		}

		return this;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable DateRangeQueryFilter filter) {
		if (filter!=null) {
			LocalDate from = filter.calculateFrom();
			LocalDate to = filter.calculateTo();

			Bson fromFilter = from==null ? null : Filters.gte(field, from);
			Bson toFilter = to==null ? null : Filters.lt(field, to);

			fromFilter = (fromFilter!=null && toFilter!=null)
				? Filters.and(fromFilter, toFilter)
				: fromFilter != null ? fromFilter : toFilter;

			if (fromFilter!=null)
				context.getFilters().add(fromFilter);
		}

		return this;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable ListQueryFilter<? extends QueryFilter> filter) {
		if (filter!=null) {
			Runnable r = () -> filter.getFilters().forEach(it -> it.accept(this, field));
			if (QueryConjunction.or.equals(filter.getConjunction()))
				or(r);
			else
				and(r);
		}

		return this;
	}

	@Nonnull
	@Override
	public MongoFilterQueryBuilder<E, P> add(@Nonnull String field, @Nullable EntityQueryFilter<?> filter) {
		throw new IllegalStateException("Not implemented, use for example SingleValueQueryFilter<ObjectId> instead.");
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
