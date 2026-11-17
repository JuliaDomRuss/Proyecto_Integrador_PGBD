from pyspark.sql import functions as F


# check if all the required columns are present
def require_columns(df, columns, name):
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing columns {missing}")

# check if the dataframe has some entries, otherwise add error message
def require_not_empty(df, name):
    if df.limit(1).count() == 0:
        raise ValueError(f"{name}: dataset is empty")

# check for duplicate entries, there can't be in our data
def require_unique(df, key_columns, name):
    duplicated = df.groupBy(*key_columns).count().filter(F.col("count") > 1)
    if duplicated.limit(1).count() > 0:
        raise ValueError(f"{name}: duplicated keys in {key_columns}")

# check for null values 
def require_no_nulls(df, columns, name):
    for column in columns:
        if df.filter(F.col(column).isNull()).limit(1).count() > 0:
            raise ValueError(f"{name}: null values in column '{column}'")


def validate_raw_exchange_rate(df):
    name = "Raw exchange rates (not transformed yet)"
    require_columns(df, ["fecha", "dato"], name)
    require_not_empty(df, name)

    bad_dates = df.filter(F.to_date("fecha", "dd/MM/yyyy").isNull()).count()
    if bad_dates > 0:
        raise ValueError(f"{name}: {bad_dates} dates not in dd/MM/yyyy format")

    bad_values = (
        df.filter(F.col("dato") != "N/E")
        .filter(F.col("dato").cast("double").isNull())
        .count()
    )
    if bad_values > 0:
        raise ValueError(f"{name}: {bad_values} values are not numeric")

    require_unique(df, ["fecha"], name)
    return df.count()


def validate_igae(df):
    name = "IGAE"
    required = ["FECHA", "ACTIVIDAD", "IGAE"]
    require_columns(df, required, name)
    require_not_empty(df, name)
    require_no_nulls(df, required, name)
    require_unique(df, ["FECHA", "ACTIVIDAD"], name)

    if df.filter(F.dayofmonth("FECHA") != 1).limit(1).count() > 0:
        raise ValueError(f"{name}: FECHA is not always the first day of the month")

    if df.filter(F.col("IGAE") <= 0).limit(1).count() > 0:
        raise ValueError(f"{name}: IGAE values <= 0")

    return df.count()
