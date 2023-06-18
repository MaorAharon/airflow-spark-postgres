import pandas as pd

def minimize_parquet(input: str, output: str, limit: int = 100):
    df = pd.read_parquet(input, engine='pyarrow')
    top_x_rows = df.head(limit)
    top_x_rows.to_parquet(output, engine='pyarrow')

def main():
    input_folder: str = "~/Downloads/3/yellow/"
    file_name: str = "yellow_tripdata_2020-02.parquet"
    output_folder: str = "~/Documents/git/python/andrejnevesjr/airflow-spark-minio-postgres/src/spark/assets/data/parquet/"
    input: str = f"{input_folder}{file_name}"
    output: str =  f"{output_folder}{file_name}"
    minimize_parquet(input, output)

if __name__ == "__main__":
    main()