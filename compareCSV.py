import csv
import re

from typing import List, Dict, Tuple, Any


def read_csv(path: str) -> List[Dict[str, str]]:
    """Read a CSV file into a list of dicts keyed by header."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def filter_rows_by_column(
    file_path: str,
    column_name: str,
    value: str,
    output_path: str | None = None,
) -> List[Dict[str, str]]:
    """Return rows where column_name == value, optionally exporting to CSV."""
    rows = read_csv(file_path)

    if not rows:
        print("No rows in file.")
        return []

    if column_name not in rows[0]:
        raise ValueError(f"Column '{column_name}' not found in {file_path}")

    filtered = [r for r in rows if r.get(column_name) == value]

    if output_path:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(filtered)
        print(f"Wrote {len(filtered)} rows to {output_path}")

    return filtered

def filter_rows_by_column_value(
    file_path: str,
    value: str = "77",
    matched_output: str | None = "attribute2_77.csv",
    unmatched_output: str | None = "attribute_not_77.csv",
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Filter by 'Attribute 2 value(s)' == value and export matched + unmatched rows.

    Returns (matched_rows, unmatched_rows)
    """
    column_name = "Attribute 2 value(s)"
    rows = read_csv(file_path)

    if not rows:
        print("No rows in file")
        return [], []
    
    if column_name not in rows[0]:
        raise ValueError(f"Column '{column_name}' not found in {file_path}")
    
    matched = [r for r in rows if r.get(column_name) == value]
    unmatched = [r for r in rows if r.get(column_name) != value]

    if matched_output:
        with open(matched_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(matched)
        print(f"Wrote {len(matched)} rows with {column_name}='{value}' to {matched_output}")
    
    if unmatched_output:
        with open(unmatched_output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(unmatched)
        print(f"Wrote {len(unmatched)} rows WITHOUT {column_name}='{value}' to {unmatched_output}")
    return matched, unmatched


def filter_rows_by_column_prefix(
    file_path: str,
    column_name: str,
    prefix: str,
    output_path: str | None = None,
    case_sensitive: bool = True,
    trim: bool = True,
) -> List[Dict[str, str]]:
    """Return rows whose column value starts with a prefix.

    Args:
        file_path: CSV to read.
        column_name: Header name to match against.
        prefix: String prefix to test.
        output_path: Optional CSV file to write results.
        case_sensitive: If False, compare using lowercased values/prefix.
        trim: If True, strip leading/trailing whitespace before comparison.
    """
    rows = read_csv(file_path)
    if not rows:
        print("No rows in file.")
        return []
    if column_name not in rows[0]:
        raise ValueError(f"Column '{column_name}' not found in {file_path}")

    if not case_sensitive:
        prefix_cmp = prefix.lower()
        def starts(val: str) -> bool:
            if trim:
                val = val.strip()
            return val.lower().startswith(prefix_cmp)
    else:
        prefix_cmp = prefix
        def starts(val: str) -> bool:
            if trim:
                val = val.strip()
            return val.startswith(prefix_cmp)

    filtered = [r for r in rows if starts(r.get(column_name, ""))]

    if output_path:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(filtered)
        print(f"Wrote {len(filtered)} rows to {output_path}")

    return filtered


def filter_rows_name_matches_cuba(
    file_path: str,
    output_path: str | None = None,
    column_name: str = "Name",
) -> List[Dict[str, str]]:
    """Filter rows whose name contains Cuba variants (cuba/cuban/cubana...).

    Matches case-insensitively on word starts: any token beginning with 'cuba'.
    Examples that match: "Cuba", "Cuban", "Cubana", "Cubanito".

    Args:
        file_path: CSV to read.
        output_path: Optional CSV file to write results.
        column_name: Column to inspect (default 'Name').

    Returns:
        List of matching row dicts.
    """

    rows = read_csv(file_path)
    if not rows:
        print("No rows in file.")
        return []
    if column_name not in rows[0]:
        raise ValueError(f"Column '{column_name}' not found in {file_path}")

    # Regex: word boundary then 'cuba' followed by zero or more word chars
    # Case-insensitive. This captures cuba, cuban, cubana, etc.
    pattern = re.compile(r"\b(cuba\w*)\b", re.IGNORECASE)

    def matches(val: str) -> bool:
        if not val:
            return False
        return pattern.search(val) is not None

    filtered = [r for r in rows if matches(r.get(column_name, ""))]

    if output_path:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(filtered)
        print(f"Wrote {len(filtered)} rows to {output_path}")

    return filtered


def index_by_keys(
    rows: List[Dict[str, str]],
    key_columns: List[str],
) -> Dict[Tuple[str, ...], Dict[str, str]]:
    """Build a dict keyed by the tuple of key_columns values."""
    if not rows:
        return {}

    for col in key_columns:
        if col not in rows[0]:
            raise ValueError(f"Key column '{col}' not found in CSV")

    index: Dict[Tuple[str, ...], Dict[str, str]] = {}
    for r in rows:
        key = tuple(r.get(col, "") for col in key_columns)
        index[key] = r
    return index


def compare_csv_by_keys(
    file1: str,
    file2: str,
    key_columns: List[str],
    compare_columns: List[str],
    diff_output_path: str | None = "differences.csv",
    only_in_1_output: str | None = "only_in_file1.csv",
    only_in_2_output: str | None = "only_in_file2.csv",
) -> None:
    """Compare two CSVs by key columns and optionally export CSV reports.

    - diff_output_path: rows (per key+column) where compare_columns differ
    - only_in_1_output: rows that exist only in file1
    - only_in_2_output: rows that exist only in file2
    """
    rows1 = read_csv(file1)
    rows2 = read_csv(file2)

    if not rows1:
        print(f"{file1} has no data.")
        return
    if not rows2:
        print(f"{file2} has no data.")
        return

    # Validate compare columns
    for col in compare_columns:
        if col not in rows1[0]:
            raise ValueError(f"Column '{col}' not found in {file1}")
        if col not in rows2[0]:
            raise ValueError(f"Column '{col}' not found in {file2}")

    idx1 = index_by_keys(rows1, key_columns)
    idx2 = index_by_keys(rows2, key_columns)

    keys1 = set(idx1.keys())
    keys2 = set(idx2.keys())

    only_in_1 = keys1 - keys2
    only_in_2 = keys2 - keys1
    in_both = keys1 & keys2

    # Export rows only in file1
    if only_in_1:
        print("Rows only in file1:")
        if only_in_1_output:
            with open(only_in_1_output, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows1[0].keys())
                writer.writeheader()
                for k in sorted(only_in_1):
                    writer.writerow(idx1[k])
                    print(f"  key={k} -> {idx1[k]}")
        else:
            for k in sorted(only_in_1):
                print(f"  key={k} -> {idx1[k]}")
        print("-" * 60)

    # Export rows only in file2
    if only_in_2:
        print("Rows only in file2:")
        if only_in_2_output:
            with open(only_in_2_output, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows2[0].keys())
                writer.writeheader()
                for k in sorted(only_in_2):
                    writer.writerow(idx2[k])
                    print(f"  key={k} -> {idx2[k]}")
        else:
            for k in sorted(only_in_2):
                print(f"  key={k} -> {idx2[k]}")
        print("-" * 60)

    # Collect column differences for matched keys
    diff_rows: List[Dict[str, Any]] = []
    for k in sorted(in_both):
        r1 = idx1[k]
        r2 = idx2[k]
        for col in compare_columns:
            v1 = r1.get(col, "")
            v2 = r2.get(col, "")
            if v1 != v2:
                # One row per mismatched key+column
                row_out: Dict[str, Any] = {}
                for i, key_col in enumerate(key_columns):
                    row_out[key_col] = k[i]
                row_out["column"] = col
                row_out["value_file1"] = v1
                row_out["value_file2"] = v2
                diff_rows.append(row_out)
                print(f"Diff key={k}, column={col}: '{v1}' vs '{v2}'")

    # Export differences
    if diff_output_path and diff_rows:
        fieldnames = key_columns + ["column", "value_file1", "value_file2"]
        with open(diff_output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(diff_rows)
        print(f"Wrote {len(diff_rows)} differences to {diff_output_path}")
    elif diff_output_path:
        print("No column differences found; diff CSV not created.")

    if not only_in_1 and not only_in_2 and not diff_rows:
        print("No differences found for the given key/compare columns.")


def main() -> None:
    """Example usage; edit paths/column names to your data."""
    # Example 1: filter by brand == "Zyn"
    # Change these values to match your CSV
    # source_csv = "data.csv"      # input file
    # brand_column = "brand"       # column header
    # brand_value = "Zyn"          # value to filter for

    # print(f"Filtering rows where {brand_column} == '{brand_value}'...")
    # filtered = filter_rows_by_column(
    #     source_csv,
    #     brand_column,
    #     brand_value,
    #     output_path="filtered_Zyn.csv",
    # )
    # print(f"Found {len(filtered)} matching rows.\n")

    # Example 1b: prefix filtering (names starting with "77")
    # Uncomment and adjust file path if needed.
    # prefix_filtered = filter_rows_by_column_prefix(
    #     "file1.csv",  # or your actual file e.g. "file.csv"
    #     "Name",
    #     "77",
    #     output_path="names_starting_77.csv",
    #     case_sensitive=False,
    # )
    # print(f"Found {len(prefix_filtered)} rows with Name starting '77'.\n")

    # matched, unmatched = filter_rows_by_column_value(
    #     "names_starting_77.csv",
    #     value = "77",
    #     matched_output = "Products_With_Brand_77_Att.csv",
    #     unmatched_output = "Products_Without_Brand_77_Att.csv"
    # )
    # print(f"Found {len(matched)} rows with 77 and {len(unmatched)} rows without 77.\n")
    

    # Example 2: compare two CSVs by key and columns
    # file1 = "file1.csv"
    # file2 = "file2.csv"
    # key_columns = ["brand"]      # identifiers used to match rows
    # compare_columns = ["price"]  # columns whose values should match

    # print("Comparing CSVs by key columns and selected fields...")
    # compare_csv_by_keys(
    #     file1,
    #     file2,
    #     key_columns,
    #     compare_columns,
    #     diff_output_path="differences.csv",
    #     only_in_1_output="only_in_file1.csv",
    #     only_in_2_output="only_in_file2.csv",
    # )

    cuba_matches = filter_rows_name_matches_cuba(
        "cuba.csv",
        output_path="cuba_matches.csv",
        column_name="Name",
    )
    print(f"Found {len(cuba_matches)} rows matching Cuba variants in Name.\n")


if __name__ == "__main__":
    main()

