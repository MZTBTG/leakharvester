import polars as pl

def test_split_logic():
    data = [
        "simple@test.com:password",
        "complex@test.com:pass:with:colons",
        "garbage_prefix:valid@test.com:pass",
        "no_separator_garbage",
        "broken:split"
    ]
    
    df = pl.DataFrame({"raw_line": data})
    
    # Logic from ingestor.py
    email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
    
    df = df.with_columns(
        pl.col("raw_line").str.splitn(":", 2).alias("split_parts")
    )
    
    df = df.with_columns([
        pl.col("split_parts").struct.field("field_0").alias("part_0"),
        pl.col("split_parts").struct.field("field_1").alias("part_1")
    ])
    
    df = df.with_columns(
            pl.when(pl.col("part_0").str.contains(email_pattern))
            .then(pl.col("part_0"))
            .otherwise(pl.col("raw_line").str.extract(email_pattern, 1))
            .alias("email"),
            
            pl.when(pl.col("part_0").str.contains(email_pattern))
            .then(pl.col("part_1"))
            .otherwise(pl.lit("")) 
            .alias("password")
    )
    
    print(df.select(["raw_line", "email", "password"]))

if __name__ == "__main__":
    test_split_logic()
