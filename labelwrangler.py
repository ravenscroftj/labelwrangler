"""Labelwrangler is a simple command line tool for tidying and wrangling CSV files

"""
import click
import re
import sys
from typing import Optional
from smart_open import open
import pandas as pd


@click.group()
def cli():
    pass

@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.option("-n", type=int, default=10, help="Number of lines to show")
def head(input_file, n):
    """Show the top N records for INPUT_FILE"""
    df = pd.read_csv(input_file)

    print(df.head(n))

@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.argument("output_file", type=click.Path(file_okay=True))
@click.option("--column", type=str, required=True)
def strip_html(input_file, output_file, column):
    """Strip HTML tags from given column and save output file"""

    df = pd.read_csv(input_file)

    if column not in df.columns:
        print(f"Column {column} not in dataframe. Valid options are: {list(df.columns)}")
        return 1

    clean = re.compile('<.*?>')
    clean_lmb = lambda x: re.sub(clean,'', x)

    df[column] = df[column].apply(clean_lmb)

    df.to_csv(output_file)
    
@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.argument("output_file", type=click.Path(file_okay=True))
@click.option("--columns", type=str, required=True)
def deduplicate(input_file, output_file, columns):
    """De-duplicate rows in CSV based on columns specified (comma separated)"""

    df = pd.read_csv(input_file)

    columns = [x.strip() for x in columns.split(",")]

    if len(columns) < 1:
        print("No columns specified")

    all_ok = True
    for column in columns:
        if column not in df.columns:
            print(f"Could not find specified column {column} in table")
            all_ok = False

    if not all_ok:
        print(f"One or more columns not found in table. Options are {list(df.columns)}")
        sys.exit(1)

    print(f"Dropping duplicate entries based on column subset{columns}")
    df.drop_duplicates(subset=columns, inplace=True)

    df.to_csv(output_file)
        

@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.option("--label-column", type=str, default=None, help="name of column containing label", required=True)
def stat(input_file, label_column):
    """Show top level stats for labels in INPUT_FILE"""
    df = pd.read_csv(input_file)
    print(df[label_column].value_counts())

@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.argument("output_file", type=click.Path(file_okay=True))
@click.option("--columns", type=str, default=None, help="comma separated list of columns to check for na", required=True)
def dropna(input_file, output_file,  columns):
    """Drop rows where a None/NA value appears in one of columns"""

    print(f"Load dataframe from {input_file}")

    df = pd.read_csv(input_file)

    after_drop = df.dropna(subset=columns.split(','))

    print(f"Removing {len(df) - len(after_drop)} columns from dataset")

    after_drop.to_csv(output_file)



@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.argument("output_file", type=click.Path(file_okay=True))
@click.option("--label-column", type=str, default=None, help="name of column containing label", required=True)
@click.option("--remove-list", type=str, default=None, help="comma separated labels to remove", required=True)
def remove(input_file, output_file,  label_column, remove_list):
    """Remove REMOVE_LIST labels from given INPUT_FILE and output to OUTPUT_FILE
    
    Takes CSV-like file at INPUT_FILE and removes any row where the label in LABEL-COLUMN 
    has a value matching the REMOVE-LIST.
    """

    print(f"Load dataframe from {input_file}")

    df = pd.read_csv(input_file)

    print(f"Checking that label column '{label_column}' exists")

    if label_column not in df.columns:
        print(f"No such column {label_column}. Valid options for this CSV are: {df.columns}")
        return 1

    print("Checking remove list")

    for lbl in remove_list.split(","):
        print(f"Found {len(df[df[label_column] == lbl])} rows where df[{label_column}]={lbl}")

    print("Removing data")
    df= df[~df[label_column].isin(remove_list.split(","))]

    print("New distribution of labels:")
    print(df[label_column].value_counts())

    df.to_csv(output_file)

@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.argument("output_file", type=click.Path(file_okay=True))
@click.option("--label-column", type=str, default=None, help="name of column containing label", required=True)
@click.option("--include-list", type=str, default=None, help="comma separated labels to merge")
@click.option("--maximum", type=int, required=True, help="Integer maximum to downsample to")
@click.option("--random-state",type=int, default=42, help="Random seed to sample against")
def random_downsample(input_file, output_file, label_column, include_list, maximum, random_state):
    """Randomly undersample examples from INPU_TFILE with labels in include-list and write new sample to OUTPUT_FILE"""

    include = include_list.split(",") if include_list is not None else []

    if len(include) < 1:
        print("You must provide at least one label to downsample")
        return 1

    print(f"Load dataframe from {input_file}")

    df = pd.read_csv(input_file)

    print("Check label column exists")
    if label_column not in df.columns:
        print(f"Could not find label column. Valid columns are {df.columns}")
        return 1

    for lbl in include:
        print(f"Found {len(df[df[label_column] == lbl])} examples for label={lbl}, downsampling...")
        sample = df[df[label_column] == lbl].sample(maximum, random_state=random_state)
        df = df[df[label_column] != lbl]
        df = df.append(sample)

        print(f"After sampling there are now  {len(df[df[label_column] == lbl])} examples for label={lbl}")


    print(f"Saving updated labels to {output_file}")
    df.to_csv(output_file)


@cli.command()
@click.argument("input_file", type=click.Path(file_okay=True))
@click.argument("output_file", type=click.Path(file_okay=True))
@click.option("--label-column", type=str, default=None, help="name of column containing label", required=True)
@click.option("--include-list", type=str, default=None, help="comma separated labels to merge")
@click.option("--exclude-list", type=str, default=None, help="comma separated labels to exclude from merge")
@click.option("--new-label-name", type=str, required=True, help="Name for new merged label")
def merge(input_file, output_file,  label_column, include_list, exclude_list, new_label_name):
    """Merge labels according to include and exclude lists.

    Take CSV-like file at INPUT_FILE and use include/exclude list to rewrite so that
    all labels in the --include-list and NOT on the --exclude-list get merged and renamed to
    the value of --new-label-name.

    E.g. I want to take a multi class CSV with an 'Other' label and turn it into a binary class dataset

    labelwrangler.py merge input.csv output.csv --label-column=Label --exclude-list=Other --new-label-name="Not Other"
    """

    include = include_list.split(",") if include_list is not None else []
    exclude = exclude_list.split(",") if exclude_list is not None else []

    if len(include) < 1 and len(exclude) < 1:
        print("You must provide at least one column to include or exclude in your merge.")
        return 1

    print(f"Load dataframe from {input_file}")

    df = pd.read_csv(input_file)

    print("Check label column exists")
    if label_column not in df.columns:
        print(f"Could not find label column. Valid columns are {df.columns}")
        return 1

    print("Check include/exclude labels exist")

    missing = 0
    labels = df[label_column].unique()
    
    for lbl in set(include + exclude):
        if lbl not in labels:
            print(f"Could not find column {lbl} in dataframe.")
            missing += 1

    if missing > 0:
        print("Exiting early due to missing columns from include/exclude filter")
        print(f"Columns in CSV are: {df.columns}")
        return 1

    if len(include) > 0 and len(exclude) > 0:
        idx = df[label_column].isin(include) and ~df[label_column].isin(exclude)
    
    elif len(exclude) > 0:
        idx =  ~df[label_column].isin(exclude)
    elif len(include) > 0:
        idx = df[label_column].isin(include)

    df.loc[idx, label_column] = new_label_name

    print(f"Saving updated labels to {output_file}")
    df.to_csv(output_file)


if __name__ == "__main__":
    cli()
