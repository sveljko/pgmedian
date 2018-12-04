CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS '$libdir/median', 'median_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_inv_transfn(state internal, val anyelement)
RETURNS internal
AS '$libdir/median', 'median_inv_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, val anyelement)
RETURNS anyelement
AS '$libdir/median', 'median_finalfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_finalfn,
    finalfunc_extra,
    msfunc = _median_transfn,
    mstype = internal,
    minvfunc = _median_inv_transfn,
    mfinalfunc = _median_finalfn,
    mfinalfunc_extra
);
