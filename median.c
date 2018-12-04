/* -*- c-file-style:"bsd"; tab-width:4; indent-tabs-mode: t -*- */
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/varlena.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/** There probably is a way to do this in a more generic way, but
    PostgreSQL `Datum` and friends are not very well documented, so,
    we're doing it "by hand", handling "classes" of values - meaning
    oids that can be handled in a same ("generic") way. Currently,
    obviously the floating point types are missing, handling of string
    types other than `text` and date/time other than `timestamp` is
    not "researched" (though might probably require very little code
    changes to support), but many other types also.
*/
enum ValueClass
{
	/** Which can be handled like integers */
	vcNumeral,
	/** Which can be handled like "Pascal" strings */
	vcText
};

/** This is a straight-forward implementation that scales poorly as
    the `data` goes "beyond cache". To solve that, we should provide
    an array of "pages", each being an array of at most 100000 or so
    elements (that can fit into a cache of a "everyday" CPU), and
    insert into them pages, adding new one if the last is full and
    element needs to be inserted after it and breaking them up when
    they are full yet a new element needs to be inserted. It would
    slow down iteration a little, but would speed up insertion _much_,
    making it possible to handle large amounts of data.
*/
struct MedianState
{
	int8		varlen_hdr_[VARHDRSZ];
	size_t		cap;
	size_t		dim;
	enum ValueClass valclass;
	union
	{
		int64		i[1];
		text	   *t[1];
	}			data;
};


static struct MedianState *
expand_if_need_be(struct MedianState *pms)
{
	if (pms->dim >= pms->cap)
	{
		size_t		ncap = (pms->cap * 3) / 2;
		size_t		to_alloc;
		struct MedianState *npms;

		if (ncap < pms->cap)
		{
			elog(ERROR, "Overflow while expanding array for median");
			return pms;
		}
		/* elog(WARNING, "pms->cap = %lu, ncap = %lu", pms->cap, ncap); */
		to_alloc = sizeof *pms + sizeof pms->data * ncap;
		/* elog(WARNING, "toalloc = %lu", to_alloc); */
		npms = repalloc(pms, to_alloc);
		if (NULL == npms)
		{
			elog(ERROR, "No memory while expanding array for median");
			return pms;
		}
		pms = npms;
		SET_VARSIZE(pms, to_alloc);
		pms->cap = ncap;
	}
	return pms;
}

static struct MedianState *
insert_median_numeral(struct MedianState *pms, int64 x)
{
	size_t		i;

	for (i = 0; i < pms->dim; ++i)
	{
		if (x >= pms->data.i[i])
		{
			memmove(pms->data.i + i + 1, pms->data.i + i, (pms->dim - i) * sizeof pms->data.i[0]);
			break;
		}
	}
	pms->data.i[i] = x;
	++pms->dim;
	return pms;
}

static int
text_cmp(text *arg1, text *arg2, Oid collid)
{
	char	   *a1p;
	char	   *a2p;
	int			len1,
				len2;

	a1p = VARDATA_ANY(arg1);
	a2p = VARDATA_ANY(arg2);

	len1 = VARSIZE_ANY_EXHDR(arg1);
	len2 = VARSIZE_ANY_EXHDR(arg2);

	return varstr_cmp(a1p, len1, a2p, len2, collid);
}

static struct MedianState *
insert_median_text(struct MedianState *pms, text *x, Oid collation)
{
	size_t		i;

	for (i = 0; i < pms->dim; ++i)
	{
		if (text_cmp(x, pms->data.t[i], collation) < 0)
		{
			memmove(pms->data.t + i + 1, pms->data.t + i, (pms->dim - i) * sizeof pms->data.t[0]);
			break;
		}
	}
	pms->data.t[i] = x;
	++pms->dim;
	return pms;
}


static struct MedianState *
create_MedianState(MemoryContext ctx)
{
	struct MedianState *pms;
	size_t const ncap = 64;
	size_t const to_alloc = sizeof *pms + sizeof pms->data * ncap;

	/*
	 * elog(WARNING, "create_MedianState() NULL == pms, to_alloc = %lu",
	 * to_alloc);
	 */
	pms = MemoryContextAllocZero(ctx, to_alloc);
	if (NULL == pms)
	{
		elog(ERROR, "create_MedianState() no memory");
		return NULL;
	}
	SET_VARSIZE(pms, to_alloc);
	pms->dim = 0;
	pms->cap = ncap;

	return pms;
}

static struct MedianState *
do_median_numeral(struct MedianState *pms, int64 x)
{
	return insert_median_numeral(expand_if_need_be(pms), x);
}


static struct MedianState *
do_median_text(struct MedianState *pms, text *x, Oid collation)
{
	return insert_median_text(expand_if_need_be(pms), x, collation);
}

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 *
 * It's also used for a moving-aggregate ("window"), though there might
 * possibly be scenarios where it should be different. Right now, we're not
 * 100% sure, as PostgreSQL docs are not very precise on this matter.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
	struct MedianState *state;

	MemoryContext agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		elog(ERROR, "median_transfn called in non-aggregate context");
		PG_RETURN_NULL();
	}
	if (PG_ARGISNULL(0))
	{
		state = NULL;			/* first element */
	}
	else
	{
		state = (struct MedianState *) PG_GETARG_BYTEA_P(0);
	}
	if (PG_ARGISNULL(1))
	{
		/* discard NULL input values */
	}
	else
	{
		Oid			partyp;

		if (NULL == state)
		{
			state = create_MedianState(agg_context);
		}
		partyp = get_fn_expr_argtype(fcinfo->flinfo, 1);
		switch (partyp)
		{
			case INT2OID:
				state = do_median_numeral(state, PG_GETARG_INT16(1));
				break;
			case INT4OID:
				state = do_median_numeral(state, PG_GETARG_INT32(1));
				break;
			case INT8OID:
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				state = do_median_numeral(state, PG_GETARG_INT64(1));
				break;
			case TEXTOID:
				state = do_median_text(state, PG_GETARG_TEXT_P_COPY(1), PG_GET_COLLATION());
				break;
			default:
				elog(ERROR, "parameter type oid=%u not supported", partyp);
		}
	}

	if (state == NULL)
	{
		PG_RETURN_NULL();
	}
	else
	{
		PG_RETURN_BYTEA_P(state);
	}
}


static struct MedianState *
remove_median_numeral(struct MedianState *pms, int64 x)
{
	size_t		i;

	/* Should do binary search here */
	for (i = 0; i < pms->dim; ++i)
	{
		if (x == pms->data.i[i])
		{
			memmove(pms->data.i + i, pms->data.i + i + 1, sizeof pms->data.i[0]);
			return pms;
		}
	}
	elog(ERROR, "remove_median_numeral(%ld) not found", x);
	return pms;
}

static struct MedianState *
remove_median_text(struct MedianState *pms, text *x, Oid collation)
{
	size_t		i;

	/* Should do binary search here */
	for (i = 0; i < pms->dim; ++i)
	{
		if (0 == text_cmp(x, pms->data.t[i], collation))
		{
			memmove(pms->data.t + i, pms->data.t + i + 1, sizeof pms->data.t[0]);
			return pms;
		}
	}
	elog(ERROR, "remove_median_text() not found");
	return pms;
}

static struct MedianState *
undo_median_numeral(struct MedianState *pms, int64 x)
{
	elog(WARNING, "undo_median_numeral(%ld)", x);
	return remove_median_numeral(expand_if_need_be(pms), x);
}


static struct MedianState *
undo_median_text(struct MedianState *pms, text *x, Oid collation)
{
	return remove_median_text(expand_if_need_be(pms), x, collation);
}

PG_FUNCTION_INFO_V1(median_inv_transfn);

/*
 * Median inverse state transfer function.
 *
 * This function is called for "moving average" (window) aggregate and is
 * designed to "remove a value from aggregate calculation".
 *
 */
Datum
median_inv_transfn(PG_FUNCTION_ARGS)
{
	struct MedianState *state;

	MemoryContext agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		elog(ERROR, "median_transfn called in non-aggregate context");
		PG_RETURN_NULL();
	}
	Assert(PG_ARGISNULL(0));
	state = (struct MedianState *) PG_GETARG_BYTEA_P(0);
	if (PG_ARGISNULL(1))
	{
		/*
		 * discard NULL input values - though I'm not sure it's possible to
		 * get a NULL for the inverse transfer function.
		 */
	}
	else
	{
		Oid			partyp;

		if (NULL == state)
		{
			state = create_MedianState(agg_context);
		}
		partyp = get_fn_expr_argtype(fcinfo->flinfo, 1);
		switch (partyp)
		{
			case INT2OID:
				state = undo_median_numeral(state, PG_GETARG_INT16(1));
				break;
			case INT4OID:
				state = undo_median_numeral(state, PG_GETARG_INT32(1));
				break;
			case INT8OID:
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				state = undo_median_numeral(state, PG_GETARG_INT64(1));
				break;
			case TEXTOID:
				state = undo_median_text(state, PG_GETARG_TEXT_P_COPY(1), PG_GET_COLLATION());
				break;
			default:
				elog(ERROR, "parameter type oid=%u not supported", partyp);
		}
	}

	if (state == NULL)
	{
		PG_RETURN_NULL();
	}
	else
	{
		PG_RETURN_BYTEA_P(state);
	}
}


PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function(s). It should perform any
 * necessary post processing and clean up any temporary state.
 *
 * This assumes that, by now, the data is sorted. An alternative
 * approach would be to use "Quick Select", probably improved, like
 * "Intro Select" and _not_ sort the data while inserting. That would
 * probably be faster on average, but, writing a good Intro (or
 * similar) Select is hard and error prone - it's very easy to end up
 * with something that's actually slower than this implementation for
 * some inputs that are of interest to a particular implementation.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
	struct MedianState *state;
	MemoryContext agg_context;

	if (!AggCheckCallContext(fcinfo, &agg_context))
	{
		elog(ERROR, "median_finalfn called in non-aggregate context");
		PG_RETURN_NULL();
	}

	if (PG_ARGISNULL(0))
	{
		state = NULL;			/* had no elements! */
	}
	else
	{
		state = (struct MedianState *) PG_GETARG_BYTEA_P(0);
	}
	if (NULL == state)
	{
		PG_RETURN_NULL();
	}
	else
	{
		if (state->dim > 0)
		{
			switch (state->valclass)
			{
				case vcNumeral:
					{
						int64		rslt = state->data.i[state->dim / 2];

						PG_RETURN_DATUM(Int64GetDatum(rslt));
					}
				case vcText:
					PG_RETURN_TEXT_P(state->data.t[state->dim / 2]);
			}
		}

		PG_RETURN_NULL();
	}
}

/* For parallel aggregates, one would need to write a few more
   functions. A "combine", essentially implementing a "merge" of
   sorted arrays and "serialze" and "de-serialize", which are trivial
   for numbers, but require some work for strings, writing each one
   with the length first and then the actual content.
 */
