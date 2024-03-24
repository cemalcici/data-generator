# region Global Packages
import pandas as pd
# endregion

# region Local Packages
import data_generator.exeptions as dgex
# endregion


# region Read Functions
def _read_csv_file(
    input_path: str,
    sep_char: str,
    shuffle: bool
) -> pd.DataFrame:
    return (
        pd.read_csv(input_path, sep=sep_char).sapmple(frac=1)
        if shuffle else pd.read_csv(input_path, sep=sep_char)
    )


def _read_parquet_file(
    input_path: str,
    shuffle: bool
) -> pd.DataFrame:
    return (
        pd.read_parquet(input_path, 'auto').sample(frac=1)
        if shuffle else pd.read_parquet(input_path, 'auto')
    )
# endregion


def read_source_file(
    input_path: str,
    sep_char: str,
    extension: str,
    shuffle: bool,
    exclude_cols: tuple
) -> pd.DataFrame:
    
    if extension == 'csv':
        dataframe = _read_csv_file(input_path, sep_char, shuffle)
    elif extension == 'parquet':
        dataframe = _read_parquet_file(input_path, shuffle)
    else:
        raise dgex.NotVaildExtensionError(('csv', 'parquet'))
    
    if exclude_cols: 
        dataframe.drop(columns=list(exclude_cols), inplace=True, errors='ignore') 
    dataframe.dropna(inplace=True)
    return dataframe
