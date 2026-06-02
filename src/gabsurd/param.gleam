//// Converts parrot Param values to pog Value for PostgreSQL queries.

import parrot/dev
import pog

/// Convert a parrot Param to a pog Value for use with PostgreSQL.
pub fn to_pog(param: dev.Param) -> pog.Value {
  case param {
    dev.ParamBool(x) -> pog.bool(x)
    dev.ParamFloat(x) -> pog.float(x)
    dev.ParamInt(x) -> pog.int(x)
    dev.ParamString(x) -> pog.text(x)
    dev.ParamBitArray(x) -> pog.bytea(x)
    dev.ParamList(x) -> pog.array(to_pog, x)
    dev.ParamNullable(x) -> pog.nullable(to_pog, x)
    dev.ParamDate(x) -> pog.calendar_date(x)
    dev.ParamTimestamp(x) -> pog.timestamp(x)
    dev.ParamDynamic(_) -> panic as "cannot convert dynamic param to pog"
  }
}
