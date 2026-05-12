"""
WMPL Capstone Project â€” ETL Pipeline
=====================================
Ingests raw Excel exports from ILS (Polaris), OverDrive/Libby, Hoopla,
Kanopy, and UAN financial systems. Cleans, standardizes, and loads into
a SQLite database with CSV backups.

Usage:
    python etl_pipeline.py [--raw-dir PATH] [--output-dir PATH]

Defaults:
    --raw-dir     ../../data/raw
    --output-dir  ../../data/processed
"""

import os
import re
import sys
import glob
import json
import sqlite3
import logging
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("wmpl_etl")

COLUMN_RENAMES = {
    "Browse Title": "title",
    "Item Barcode": "item_barcode",
    "Collection Abbr": "collection_code",
    "Call Number": "call_number",
    "ISBN": "isbn",
    "First Available Date": "first_available_date",
    "Last Circ Transaction Date": "last_circ_date",
    "Item Status Descr": "item_status",
    "Item Lifetime Circ Count": "lifetime_circ",
    "Item Lifetime Renewals Count": "lifetime_renewals",
    "Prev Year (2025) Circ Count": "prev_year_circ",
    "Prev Year (2025) Renewals Count": "prev_year_renewals",
    "Item YTD (2026) Circ Count": "ytd_circ",
    "Item YTD (2026) Renewals Count": "ytd_renewals",
    "Item Stat Code Descr": "stat_code",
    "Material Type Description": "material_type",
}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def find_files(directory, pattern="*.xlsx"):
    return sorted(glob.glob(os.path.join(directory, "**", pattern), recursive=True))


def safe_read_excel(filepath, sheet_name=0, header=0, **kwargs):
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name, header=header, **kwargs)
        log.info(f"  Read {len(df):,} rows from '{os.path.basename(filepath)}' (sheet: {sheet_name})")
        return df
    except Exception as e:
        log.error(f"  Failed to read '{filepath}': {e}")
        return pd.DataFrame()


def clean_column_names(df):
    df.columns = [re.sub(r'\s+', '_', str(col).strip()).lower() for col in df.columns]
    return df


def clean_isbn(series):
    return (
        series.astype(str)
        .str.replace(r'[\s\-]', '', regex=True)
        .str.strip()
        .replace({'nan': None, 'None': None, '': None})
    )


def parse_dates_safe(series):
    return pd.to_datetime(series, errors='coerce')


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def get_fy(fname):
    if "2024" in fname: return "2024"
    if "2025" in fname: return "2025"
    return "unknown"


def normalize_for_match(s):
    """Normalize a string for fuzzy filename matching by removing separators."""
    return s.lower().replace("_", "").replace(" ", "").replace("-", "")


def search_paths(raw_dir, subfolder):
    return [os.path.join(raw_dir, subfolder), raw_dir]


def find_pdf(raw_dir, subfolder, name_pattern):
    """Search for a PDF file matching *name_pattern* (case-insensitive).
    Returns the first match or None."""
    for search_dir in search_paths(raw_dir, subfolder):
        for filepath in sorted(glob.glob(os.path.join(search_dir, "**", "*.pdf"), recursive=True)):
            if name_pattern.lower() in os.path.basename(filepath).lower():
                return filepath
    return None


def find_pdfs(raw_dir, subfolder, name_pattern):
    """Return all PDFs matching *name_pattern* (case-insensitive)."""
    results, seen = [], set()
    for search_dir in search_paths(raw_dir, subfolder):
        for filepath in sorted(glob.glob(os.path.join(search_dir, "**", "*.pdf"), recursive=True)):
            fname = os.path.basename(filepath)
            if fname in seen:
                continue
            if name_pattern.lower() in fname.lower():
                seen.add(fname)
                results.append(filepath)
    return results


def _pdf_extract_lines(filepath):
    """Extract all text lines from a PDF using pdfplumber."""
    all_lines = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_lines.extend(text.split('\n'))
    return all_lines


def _parse_dollar(s):
    """Parse a dollar string like '$1,234.56' to float."""
    if not s:
        return None
    cleaned = s.replace('$', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Physical Collection (ILS) Processing
# ---------------------------------------------------------------------------

def process_physical_items(raw_dir):
    log.info("=" * 60)
    log.info("PROCESSING: Physical Collection Items (ILS)")
    log.info("=" * 60)

    physical_dir = os.path.join(raw_dir, "physical")
    files = find_files(physical_dir)
    if not files:
        log.warning(f"No files found in {physical_dir}")
        return pd.DataFrame(), pd.DataFrame()

    all_items, all_keys = [], []
    # Match against normalized filenames (lowercase, spaces/special chars removed)
    category_map = {
        "adultphysicalaudiobook": "adult_audiobooks",
        "adultphysicalbook": "adult_books",
        "childrenteenphysicalaudiobook": "children_teen_audiobooks",
        "childrenteenphysicalbook": "children_teen_books",
        "dvdbluray": "dvd_bluray",
        "dvd&blu": "dvd_bluray",
    }

    for filepath in files:
        fname = os.path.basename(filepath).lower()
        fname_normalized = re.sub(r'[^a-z0-9]', '', fname)  # strip everything but letters/numbers

        # Skip the Circulation Totals file -- it's processed separately
        if 'circulation' in fname_normalized and 'total' in fname_normalized:
            continue

        category = "unknown"
        for pattern, cat in category_map.items():
            if pattern in fname_normalized:
                category = cat
                break

        log.info(f"\nProcessing: {os.path.basename(filepath)} -> category: {category}")
        wb = pd.ExcelFile(filepath)
        df = safe_read_excel(filepath, sheet_name=wb.sheet_names[0], header=0)
        if df.empty:
            continue

        df = df.rename(columns=COLUMN_RENAMES)
        df = clean_column_names(df)
        df['collection_category'] = category
        df['source_file'] = os.path.basename(filepath)

        if 'isbn' in df.columns:
            df['isbn'] = clean_isbn(df['isbn'])
        for dc in ['first_available_date', 'last_circ_date']:
            if dc in df.columns:
                df[dc] = parse_dates_safe(df[dc])
        for nc in ['lifetime_circ', 'lifetime_renewals', 'prev_year_circ',
                    'prev_year_renewals', 'ytd_circ', 'ytd_renewals']:
            if nc in df.columns:
                df[nc] = pd.to_numeric(df[nc], errors='coerce').fillna(0).astype(int)

        df = df.dropna(how='all')
        all_items.append(df)

        if 'Key' in wb.sheet_names:
            key_df = safe_read_excel(filepath, sheet_name='Key')
            key_df = key_df.dropna(how='all')
            # Find the header row containing 'Collection Code'
            header_found = False
            for i, row in key_df.iterrows():
                if any('Collection Code' in str(v) for v in row.values if v is not None):
                    # Only keep the first two columns (Collection Code, Collection Name)
                    key_df = key_df.iloc[i+1:, :2].reset_index(drop=True)
                    key_df.columns = ['collection_code', 'collection_name']
                    header_found = True
                    break
            if not header_found:
                # If no header row found, assume first two columns are code and name
                key_df = key_df.iloc[:, :2]
                key_df.columns = ['collection_code', 'collection_name']
            key_df = key_df.dropna(how='all')
            key_df['collection_category'] = category
            key_df['source_file'] = os.path.basename(filepath)
            all_keys.append(key_df)

    items_df = pd.concat(all_items, ignore_index=True) if all_items else pd.DataFrame()
    keys_df = pd.concat(all_keys, ignore_index=True) if all_keys else pd.DataFrame()
    log.info(f"\nPhysical items: {len(items_df):,} rows | Collection keys: {len(keys_df):,} rows")
    return items_df, keys_df


def process_circulation_totals(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Circulation Totals")
    log.info("=" * 60)

    filepath = None
    for d in search_paths(raw_dir, "physical"):
        for f in find_files(d):
            if 'circulation' in os.path.basename(f).lower().replace("_", " ") and 'total' in os.path.basename(f).lower():
                filepath = f
                break
        if filepath: break

    if not filepath:
        log.warning("Circulation Totals file not found")
        return pd.DataFrame()

    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
              'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    all_circ = []
    wb = pd.ExcelFile(filepath)

    for sheet_name in wb.sheet_names:
        log.info(f"\nProcessing sheet: {sheet_name}")
        df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
        year = str(sheet_name).strip()

        for idx, row in df.iterrows():
            if idx == 0: continue
            category = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            if not category or category in ['None', 'nan']: continue
            if 'Circulation' in category and 'Total' not in category: continue

            for m_idx, month in enumerate(months):
                val = row.iloc[m_idx + 1] if m_idx + 1 < len(row) else None
                circ_count = pd.to_numeric(val, errors='coerce')
                if pd.notna(circ_count):
                    all_circ.append({
                        'year': year, 'month': month, 'month_num': m_idx + 1,
                        'category': category.strip(), 'circulation_count': int(circ_count),
                        'source_file': os.path.basename(filepath),
                    })

            total_val = row.iloc[13] if len(row) > 13 else None
            total_count = pd.to_numeric(total_val, errors='coerce')
            if pd.notna(total_count):
                all_circ.append({
                    'year': year, 'month': 'total', 'month_num': 0,
                    'category': category.strip(), 'circulation_count': int(total_count),
                    'source_file': os.path.basename(filepath),
                })

    circ_df = pd.DataFrame(all_circ)
    log.info(f"\nCirculation totals: {len(circ_df):,} rows (long format)")
    return circ_df


# ---------------------------------------------------------------------------
# Digital Content Processing
# ---------------------------------------------------------------------------

def process_overdrive_detail(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: OverDrive/Libby Title Detail")
    log.info("=" * 60)

    digital_dir = os.path.join(raw_dir, "digital")
    patterns = [("ODLOverdrive_eBooks_Detail", "ebook"), ("ODLOverdrive_Audiobooks_Detail", "audiobook")]
    all_detail, seen = [], set()

    for name_pattern, format_type in patterns:
        for search_dir in search_paths(raw_dir, "digital"):
            for filepath in find_files(search_dir):
                fname = os.path.basename(filepath)
                if fname in seen: continue
                if normalize_for_match(name_pattern) in normalize_for_match(fname):
                    seen.add(fname)
                    log.info(f"\nProcessing: {fname} -> format: {format_type}")
                    fy = get_fy(fname)

                    wb = pd.ExcelFile(filepath)
                    main_sheet = [s for s in wb.sheet_names if 'title' in s.lower() or 'status' in s.lower()]
                    sheet = main_sheet[0] if main_sheet else wb.sheet_names[0]
                    df = safe_read_excel(filepath, sheet_name=sheet, header=0)
                    if df.empty: continue

                    df = clean_column_names(df)
                    df = df.dropna(how='all')
                    if 'isbn' in df.columns: df['isbn'] = clean_isbn(df['isbn'])

                    for dc in ['date_added_to_site', 'street_date', 'latest_checkout', 'last_copy_expires', 'cpc_end_date']:
                        if dc in df.columns: df[dc] = parse_dates_safe(df[dc])
                    for nc in ['owned', 'licenses_owned', 'licenses_left', 'licenses_used',
                               'active_checkouts', 'all_checkouts', 'holds', 'all_holds', 'turnover_rate']:
                        if nc in df.columns: df[nc] = pd.to_numeric(df[nc], errors='coerce')

                    price_col = [c for c in df.columns if 'library_price' in c.replace(' ', '_')]
                    if price_col:
                        df['library_price'] = pd.to_numeric(
                            df[price_col[0]].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
                        if price_col[0] != 'library_price': df = df.drop(columns=[price_col[0]])

                    df['format_type'] = format_type
                    df['fiscal_year'] = fy
                    df['source_file'] = fname
                    all_detail.append(df)

    detail_df = pd.concat(all_detail, ignore_index=True) if all_detail else pd.DataFrame()
    log.info(f"\nOverDrive detail total: {len(detail_df):,} rows")
    return detail_df


def process_overdrive_checkouts(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: OverDrive/Libby Checkouts")
    log.info("=" * 60)

    patterns = [("eBook_Checkouts", "ebook"), ("Audiobook_Checkouts", "audiobook")]
    all_checkouts, seen = [], set()

    for name_pattern, format_type in patterns:
        for search_dir in search_paths(raw_dir, "digital"):
            for filepath in find_files(search_dir):
                fname = os.path.basename(filepath)
                if fname in seen: continue
                if normalize_for_match(name_pattern) in normalize_for_match(fname):
                    seen.add(fname)
                    log.info(f"\nProcessing: {fname} -> format: {format_type}")
                    fy = get_fy(fname)

                    df = safe_read_excel(filepath, sheet_name=0, header=0)
                    if df.empty: continue
                    df = clean_column_names(df)
                    df = df.dropna(how='all')
                    if 'isbn' in df.columns: df['isbn'] = clean_isbn(df['isbn'])
                    if 'checked_out' in df.columns: df['checked_out'] = parse_dates_safe(df['checked_out'])
                    if 'date_added_to_site' in df.columns: df['date_added_to_site'] = parse_dates_safe(df['date_added_to_site'])

                    for prefix in ['adv', 'cons']:
                        for suffix in ['own', 'license_purchased', 'license_left', 'license_used']:
                            col = f"{prefix}_{suffix}"
                            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')

                    df['format_type'] = format_type
                    df['fiscal_year'] = fy
                    df['source_file'] = fname
                    all_checkouts.append(df)

    checkouts_df = pd.concat(all_checkouts, ignore_index=True) if all_checkouts else pd.DataFrame()
    log.info(f"\nOverDrive checkouts total: {len(checkouts_df):,} rows")
    return checkouts_df


def process_overdrive_purchase_orders(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: OverDrive Purchase Orders")
    log.info("=" * 60)

    all_po, seen = [], set()
    for search_dir in search_paths(raw_dir, "digital"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if fname in seen: continue
            if 'purchase_order' in fname.lower().replace(" ", "_").replace("-", "_"):
                seen.add(fname)
                log.info(f"\nProcessing: {fname}")
                fy = get_fy(fname)
                df = safe_read_excel(filepath, sheet_name=0, header=0)
                if df.empty: continue
                df = clean_column_names(df)
                df = df.dropna(how='all')

                for col in ['standard_total_(usd)', 'preorder_total_(usd)']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
                df = df.rename(columns={'standard_total_(usd)': 'standard_total_usd', 'preorder_total_(usd)': 'preorder_total_usd'})
                if 'order_date' in df.columns: df['order_date'] = parse_dates_safe(df['order_date'])
                for col in ['standard_units', 'preorder_units']:
                    if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                df['fiscal_year'] = fy
                df['source_file'] = fname
                all_po.append(df)

    po_df = pd.concat(all_po, ignore_index=True) if all_po else pd.DataFrame()
    log.info(f"\nOverDrive purchase orders total: {len(po_df):,} rows")
    return po_df


def process_hoopla_summary(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Hoopla Monthly Summary")
    log.info("=" * 60)

    all_hoopla, seen = [], set()
    for search_dir in search_paths(raw_dir, "digital"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if fname in seen: continue
            if ('hoopla' in fname.lower() and 'detail' not in fname.lower() and re.search(r'20\d{2}', fname)):
                seen.add(fname)
                log.info(f"\nProcessing: {fname}")
                fy = get_fy(fname)
                df = safe_read_excel(filepath, sheet_name=0, header=2)
                if df.empty: continue
                df = clean_column_names(df)
                df = df.dropna(how='all')

                for col in ['start_date', 'end_date']:
                    if col in df.columns: df[col] = parse_dates_safe(df[col])

                formats = ['audiobook', 'binge_pass', 'comic', 'ebook', 'movie', 'music', 'television']
                long_rows = []
                for _, row in df.iterrows():
                    for fmt in formats:
                        circs = pd.to_numeric(row.get(f"{fmt}_circulations"), errors='coerce')
                        cost = pd.to_numeric(row.get(f"{fmt}_cost"), errors='coerce')
                        avg_cost = pd.to_numeric(row.get(f"{fmt}_average_cost"), errors='coerce')
                        if pd.notna(circs) or pd.notna(cost):
                            long_rows.append({
                                'start_date': row.get('start_date'), 'end_date': row.get('end_date'),
                                'format': fmt, 'average_cost': avg_cost,
                                'circulations': int(circs) if pd.notna(circs) else 0,
                                'cost': cost, 'fiscal_year': fy, 'source_file': fname,
                            })
                    total_circs = pd.to_numeric(row.get('total_circulations'), errors='coerce')
                    total_cost = pd.to_numeric(row.get('total_cost'), errors='coerce')
                    if pd.notna(total_circs):
                        long_rows.append({
                            'start_date': row.get('start_date'), 'end_date': row.get('end_date'),
                            'format': 'total', 'average_cost': None,
                            'circulations': int(total_circs), 'cost': total_cost,
                            'fiscal_year': fy, 'source_file': fname,
                        })
                if long_rows:
                    all_hoopla.append(pd.DataFrame(long_rows))

    hoopla_df = pd.concat(all_hoopla, ignore_index=True) if all_hoopla else pd.DataFrame()
    log.info(f"\nHoopla summary total: {len(hoopla_df):,} rows (long format)")
    return hoopla_df


def process_hoopla_detail(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Hoopla Title Detail")
    log.info("=" * 60)

    all_detail, seen = [], set()
    for search_dir in search_paths(raw_dir, "digital"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if fname in seen: continue
            if 'hoopla' in fname.lower() and 'detail' in fname.lower():
                seen.add(fname)
                log.info(f"\nProcessing: {fname}")
                fy = get_fy(fname)
                df = safe_read_excel(filepath, sheet_name=0, header=0)
                if df.empty: continue
                df = clean_column_names(df)
                df = df.dropna(how='all')
                if 'isbn' in df.columns: df['isbn'] = clean_isbn(df['isbn'])
                for col in ['cost_per_circ', 'cost']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
                if 'circs' in df.columns: df['circs'] = pd.to_numeric(df['circs'], errors='coerce')
                if 'title_rank_by_circulation' in df.columns:
                    df['title_rank_by_circulation'] = pd.to_numeric(df['title_rank_by_circulation'], errors='coerce')
                df['fiscal_year'] = fy
                df['source_file'] = fname
                all_detail.append(df)

    detail_df = pd.concat(all_detail, ignore_index=True) if all_detail else pd.DataFrame()
    log.info(f"\nHoopla detail total: {len(detail_df):,} rows")
    return detail_df


def process_kanopy(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Kanopy")
    log.info("=" * 60)

    for search_dir in search_paths(raw_dir, "digital"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if 'kanopy' in fname.lower():
                log.info(f"\nProcessing: {fname}")
                df = safe_read_excel(filepath, sheet_name='Worksheet', header=0)
                if df.empty: continue
                df = clean_column_names(df)
                df = df.dropna(how='all')
                if 'total' in df.columns:
                    df['total'] = pd.to_numeric(df['total'].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
                if 'invoice_date' in df.columns: df['invoice_date'] = parse_dates_safe(df['invoice_date'])
                df['source_file'] = fname
                log.info(f"\nKanopy total: {len(df):,} rows")
                return df

    log.warning("Kanopy file not found")
    return pd.DataFrame()


def process_digital_content_usage(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Digital Content Usage Summary")
    log.info("=" * 60)

    filepath = None
    for search_dir in search_paths(raw_dir, "digital"):
        for f in find_files(search_dir):
            if 'digital_content_usage' in os.path.basename(f).lower().replace(" ", "_").replace("-", "_"):
                filepath = f
                break
        if filepath: break

    if not filepath:
        log.warning("Digital Content Usage file not found")
        return pd.DataFrame()

    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    format_categories = ['EBOOKS', 'EAUDIOBOOKS', 'EMUSIC', 'MAGAZINES', 'EVIDEO']
    all_usage = []
    wb = pd.ExcelFile(filepath)

    for sheet_name in wb.sheet_names:
        log.info(f"\nProcessing sheet: {sheet_name}")
        df = pd.read_excel(filepath, sheet_name=sheet_name, header=None)
        year = str(sheet_name).strip()
        current_format_category = None

        for idx, row in df.iterrows():
            if idx == 0: continue
            label = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
            if not label or label in ['None', 'nan']: continue

            if label.upper() in format_categories:
                current_format_category = label.upper()
                continue
            if 'total' in label.lower():
                current_format_category = None

            platform = label.strip()
            for m_idx, month in enumerate(months):
                val = row.iloc[m_idx + 1] if m_idx + 1 < len(row) else None
                usage_count = pd.to_numeric(val, errors='coerce')
                if pd.notna(usage_count):
                    all_usage.append({
                        'year': year, 'month': month, 'month_num': m_idx + 1,
                        'format_category': current_format_category,
                        'platform_or_label': platform, 'usage_count': int(usage_count),
                        'source_file': os.path.basename(filepath),
                    })

            if 13 < len(row):
                total_val = pd.to_numeric(row.iloc[13], errors='coerce')
                if pd.notna(total_val):
                    all_usage.append({
                        'year': year, 'month': 'total', 'month_num': 0,
                        'format_category': current_format_category,
                        'platform_or_label': platform, 'usage_count': int(total_val),
                        'source_file': os.path.basename(filepath),
                    })

    usage_df = pd.DataFrame(all_usage)
    log.info(f"\nDigital content usage total: {len(usage_df):,} rows (long format)")
    return usage_df


# ---------------------------------------------------------------------------
# Financial (UAN) Processing — PDF-first with Excel fallback
# ---------------------------------------------------------------------------

def _parse_fund_summary_pdf(filepath, fy):
    """Parse a UAN Fund Summary PDF into a list of dicts."""
    log.info(f"  [PDF] Reading {os.path.basename(filepath)}")
    lines = _pdf_extract_lines(filepath)
    records = []

    # Column headers in the PDF:
    # Fund # | Fund Name | Starting Fund Balance | MTD Revenue | YTD Revenue |
    # MTD Expenditures | YTD Expenditures | Ending Fund Balance |
    # Current Reserve for Encumbrance | Unencumbered Fund Balance

    for line in lines:
        line = line.strip()
        # Skip headers, totals, and metadata
        if not line or line.startswith('Fund #') or line.startswith('Report Total') or \
           line.startswith('Last reconciled') or line.startswith('Fund Summary') or \
           line.startswith('WRIGHT MEMORIAL') or line.startswith('Report reflects') or \
           line.startswith('UAN'):
            continue

        # Fund data rows start with a fund number (e.g. "1000 General $3,844...")
        m = re.match(r'^(\d{4})\s+(.+?)\s+(\$[\d,.]+(?:\s+\$[\d,.]+)*)\s*$', line)
        if not m:
            continue

        fund_num = int(m.group(1))
        fund_name = m.group(2).strip()
        dollars_str = m.group(3)

        # Extract all dollar amounts
        amounts = [_parse_dollar(d) for d in re.findall(r'\$[\d,.]+', dollars_str)]

        col_names = ['starting_balance', 'mtd_revenue', 'ytd_revenue',
                     'mtd_expenditures', 'ytd_expenditures', 'ending_balance',
                     'reserve_for_encumbrance', 'unencumbered_balance']

        record = {'fiscal_year': fy, 'fund_number': fund_num,
                  'fund_name': fund_name, 'source_file': os.path.basename(filepath)}
        for i, col in enumerate(col_names):
            record[col] = amounts[i] if i < len(amounts) else None
        records.append(record)

    log.info(f"  [PDF] Parsed {len(records)} fund rows")
    return records


def _parse_approp_ledger_pdf(filepath, fy):
    """Parse a UAN Appropriation Ledger PDF.

    Returns (transactions_list, headers_list) where each element is a
    list of dicts ready for pd.DataFrame().
    """
    log.info(f"  [PDF] Reading {os.path.basename(filepath)}")
    lines = _pdf_extract_lines(filepath)
    fname = os.path.basename(filepath)

    account_code, fund, account_name = None, None, None
    transactions, headers = [], []

    # Patterns
    dollar4_re = re.compile(r'(\$[\d,.]+)\s+(\$[\d,.]+)\s+(\$[\d,.]+)\s+(\$[\d,.]+)\s*$')
    txn_start_re = re.compile(r'^(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+(\d+)\s+')
    po_re = re.compile(r'PO\s+\d+-\d{4}')
    payment_re = re.compile(r'(\d+-?\d*)\s+(AW|CH)\s*$')

    APPROP_LABELS = {
        'Temporary Appropriation:': 'Temporary Appropriation',
        'Original Appropriation:': 'Original Appropriation',
        'Permanent Appropriation:': 'Permanent Appropriation',
        'Final Appropriation:': 'Final Appropriation',
        'Report Beginning Balance:': 'Report Beginning Balance',
        'Reserved for Encumbrance 12/31:': 'Reserved for Encumbrance 12/31',
        'Reserved for Encumbrance 12/31 Adjustment:': 'Reserved for Encumbrance 12/31 Adjustment',
    }

    for line in lines:
        line = line.strip()

        # --- Skip page headers / footers ---
        if not line or line.startswith('WRIGHT MEMORIAL') or \
           line.startswith('Appropriation Ledger') or \
           line.startswith('By Fund') or line.startswith('Year ') or \
           line.startswith('Report reflects') or \
           line.startswith('Post ') or line.startswith('Date ') or \
           line == 'Balance':
            continue

        # --- Account Code ---
        m = re.match(r'Account Code:\s*(\d{4}-\d{3}-\d{3}-\d{4})', line)
        if m:
            account_code = m.group(1)
            continue

        # --- Fund ---
        m = re.match(r'Fund:\s*(.+?)(?:\s+Reserved for)', line)
        if m:
            fund = m.group(1).strip()
            # Also capture the encumbrance amount on the same line
            enc_match = re.search(r'Reserved for Encumbrance 12/31:\s*(\$[\d,.]+)', line)
            if enc_match:
                headers.append({
                    'fiscal_year': fy, 'account_code': account_code,
                    'fund': fund, 'account_name': account_name,
                    'label': 'Reserved for Encumbrance 12/31',
                    'amount': _parse_dollar(enc_match.group(1)),
                    'source_file': fname,
                })
            continue

        # --- Account Name ---
        m = re.match(r'Account Name:\s*(.+?)(?:\s+Reserved for)', line)
        if m:
            account_name = m.group(1).strip()
            # Capture adjustment amount if on the same line
            adj_match = re.search(r'Reserved for Encumbrance 12/31 Adjustment:\s*(\$[\d,.]+)', line)
            if adj_match:
                headers.append({
                    'fiscal_year': fy, 'account_code': account_code,
                    'fund': fund, 'account_name': account_name,
                    'label': 'Reserved for Encumbrance 12/31 Adjustment',
                    'amount': _parse_dollar(adj_match.group(1)),
                    'source_file': fname,
                })
            continue

        # --- Appropriation header lines ---
        matched_label = None
        for label_text, label_clean in APPROP_LABELS.items():
            if line.startswith(label_text) or line.startswith(label_clean):
                matched_label = label_clean
                break
        if matched_label:
            amt_match = re.search(r'\$[\d,.]+', line)
            headers.append({
                'fiscal_year': fy, 'account_code': account_code,
                'fund': fund, 'account_name': account_name,
                'label': matched_label,
                'amount': _parse_dollar(amt_match.group()) if amt_match else None,
                'source_file': fname,
            })
            continue

        # --- Account Total / YTD Total / Report Total — skip ---
        if 'Account Total:' in line or 'Account YTD Total:' in line or \
           'Report Total' in line or 'Fund Total' in line or \
           'Fund YTD Total' in line:
            continue

        # --- Transaction rows ---
        m_txn = txn_start_re.match(line)
        if not m_txn:
            continue

        post_date = m_txn.group(1)
        txn_date = m_txn.group(2)
        process_id = m_txn.group(3)

        # Extract 4 dollar amounts from the end
        m_dollars = dollar4_re.search(line)
        if not m_dollars:
            continue

        expenditure = _parse_dollar(m_dollars.group(1))
        debit = _parse_dollar(m_dollars.group(2))
        credit = _parse_dollar(m_dollars.group(3))
        unencumbered = _parse_dollar(m_dollars.group(4))

        # Middle text: between process_id and first dollar sign
        after_pid = line[m_txn.end():]
        first_dollar = after_pid.find('$')
        middle = after_pid[:first_dollar].strip() if first_dollar >= 0 else after_pid.strip()

        # Split middle into vendor/payee, purpose, PO/BC, payment
        vendor_payee = middle
        purpose = None
        po_bc = None
        payment_receipt = None

        # Extract PO/BC
        po_match = po_re.search(middle)
        if po_match:
            po_bc = po_match.group()
            # Extract payment/receipt after PO
            remainder_after_po = middle[po_match.end():].strip()
            pay_match = payment_re.search(remainder_after_po)
            if pay_match:
                payment_receipt = pay_match.group().strip()
            # Everything before PO is vendor + purpose
            vendor_payee = middle[:po_match.start()].strip()
        else:
            # No PO — check for payment at end
            pay_match = payment_re.search(middle)
            if pay_match:
                payment_receipt = pay_match.group().strip()
                vendor_payee = middle[:pay_match.start()].strip()

        transactions.append({
            'fiscal_year': fy,
            'account_code': account_code,
            'fund': fund,
            'account_name': account_name,
            'post_date': post_date,
            'transaction_date': txn_date,
            'process_id': process_id,
            'vendor_payee': vendor_payee if vendor_payee else None,
            'purpose': purpose,
            'po_bc': po_bc,
            'payment_receipt_number': payment_receipt,
            'expenditure': expenditure,
            'debit': debit,
            'credit': credit,
            'unencumbered_balance': unencumbered,
            'source_file': fname,
        })

    log.info(f"  [PDF] Parsed {len(transactions)} transactions, {len(headers)} header lines")
    return transactions, headers


def process_fund_summary(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Fund Summary (UAN)")
    log.info("=" * 60)

    all_funds = []

    # --- Try PDFs first ---
    if HAS_PDFPLUMBER:
        pdf_files = find_pdfs(raw_dir, "financial", "fund_summary")
        if pdf_files:
            for filepath in pdf_files:
                fy = get_fy(os.path.basename(filepath))
                all_funds.extend(_parse_fund_summary_pdf(filepath, fy))

    # --- Fall back to Excel if no PDF results ---
    if not all_funds:
        if HAS_PDFPLUMBER:
            log.info("  No PDF files found, falling back to Excel")
        seen = set()
        for search_dir in search_paths(raw_dir, "financial"):
            for filepath in find_files(search_dir):
                fname = os.path.basename(filepath)
                if fname in seen: continue
                if 'fund_summary' in fname.lower().replace(" ", "_").replace("-", "_"):
                    seen.add(fname)
                    log.info(f"\nProcessing (Excel): {fname}")
                    fy = get_fy(fname)
                    df = pd.read_excel(filepath, sheet_name=0, header=None)

                    # --- Locate the header row(s) ---
                    # UAN Fund Summary has a two-row header:
                    #   Row 4: "Starting" ... "Month To Date" ... "Year To Date" ... "Ending Fund" ... "Reserve for" ... "Unencumbered"
                    #   Row 5: "Fund #"   ... "Fund Name"     ... "Revenue"      ... "Expenditures" ...
                    # We use the row containing "Fund #" as the anchor.
                    header_row = None
                    for idx, row in df.iterrows():
                        row_str = ' '.join(str(v) for v in row.values if pd.notna(v))
                        if 'Fund #' in row_str or 'Fund Name' in row_str:
                            header_row = idx
                            break

                    if header_row is None:
                        log.warning(f"  Could not find header row in {fname}")
                        continue

                    # --- Build dynamic column map from header ---
                    # UAN Excel spreads data across many columns with gaps
                    # (e.g. col 0=Fund#, 2=FundName, 5=FundBalance, 7=Revenue, ...).
                    # We detect positions dynamically from both header rows.
                    header_row_vals = df.iloc[header_row]
                    prev_row_vals = df.iloc[header_row - 1] if header_row > 0 else pd.Series()

                    # Merge header labels from both rows for context
                    col_positions = {}
                    for ci in range(len(header_row_vals)):
                        label_bottom = str(header_row_vals.iloc[ci]).strip().lower() if pd.notna(header_row_vals.iloc[ci]) else ''
                        label_top = ''
                        if header_row > 0 and ci < len(prev_row_vals):
                            label_top = str(prev_row_vals.iloc[ci]).strip().lower() if pd.notna(prev_row_vals.iloc[ci]) else ''
                        combined = f"{label_top} {label_bottom}".strip()

                        if 'fund #' in combined or label_bottom == 'fund #':
                            col_positions['fund_number'] = ci
                        elif 'fund name' in combined or label_bottom == 'fund name':
                            col_positions['fund_name'] = ci
                        elif 'starting' in combined and ('fund' in combined or 'balance' in combined):
                            col_positions['starting_balance'] = ci
                        elif 'ending' in combined and ('fund' in combined or 'balance' in combined):
                            col_positions['ending_balance'] = ci
                        elif 'unencumbered' in combined:
                            col_positions['unencumbered_balance'] = ci
                        elif 'reserve' in combined or 'encumbrance' in combined:
                            col_positions['reserve_for_encumbrance'] = ci
                        elif 'month to date' in label_top and 'revenue' in label_bottom:
                            col_positions['mtd_revenue'] = ci
                        elif 'year to date' in label_top and 'revenue' in label_bottom:
                            col_positions['ytd_revenue'] = ci
                        elif 'month to date' in label_top and 'expenditure' in label_bottom:
                            col_positions['mtd_expenditures'] = ci
                        elif 'year to date' in label_top and 'expenditure' in label_bottom:
                            col_positions['ytd_expenditures'] = ci
                        elif 'revenue' in label_bottom and 'mtd_revenue' not in col_positions:
                            col_positions['mtd_revenue'] = ci
                        elif 'revenue' in label_bottom and 'ytd_revenue' not in col_positions:
                            col_positions['ytd_revenue'] = ci
                        elif 'expenditure' in label_bottom and 'mtd_expenditures' not in col_positions:
                            col_positions['mtd_expenditures'] = ci
                        elif 'expenditure' in label_bottom and 'ytd_expenditures' not in col_positions:
                            col_positions['ytd_expenditures'] = ci

                    log.info(f"  Detected column positions: {col_positions}")

                    # Fallback: if dynamic detection found too few columns,
                    # collect non-NaN numeric values sequentially.
                    use_dynamic = len(col_positions) >= 4

                    # --- Financial field names for sequential fallback ---
                    financial_fields = [
                        'starting_balance', 'mtd_revenue', 'ytd_revenue',
                        'mtd_expenditures', 'ytd_expenditures', 'ending_balance',
                        'reserve_for_encumbrance', 'unencumbered_balance'
                    ]

                    for idx in range(header_row + 1, len(df)):
                        row = df.iloc[idx]
                        fund_num_col = col_positions.get('fund_number', 0)
                        fund_num = row.iloc[fund_num_col]
                        if pd.isna(fund_num): continue
                        if 'total' in str(fund_num).lower() or 'reconcil' in str(fund_num).lower(): continue
                        fund_num = pd.to_numeric(fund_num, errors='coerce')
                        if pd.isna(fund_num): continue

                        name_col = col_positions.get('fund_name', 1)
                        fund_name_val = row.iloc[name_col] if name_col < len(row) else None

                        record = {
                            'fiscal_year': fy,
                            'fund_number': int(fund_num),
                            'fund_name': str(fund_name_val).strip() if pd.notna(fund_name_val) else None,
                            'source_file': fname,
                        }

                        if use_dynamic:
                            for field in financial_fields:
                                ci = col_positions.get(field)
                                if ci is not None and ci < len(row):
                                    record[field] = pd.to_numeric(row.iloc[ci], errors='coerce')
                                else:
                                    record[field] = None
                        else:
                            # Sequential fallback: gather all non-NaN numerics
                            # starting after the fund_name column.
                            start_col = max(fund_num_col, name_col) + 1
                            numerics = []
                            for ci in range(start_col, len(row)):
                                val = pd.to_numeric(row.iloc[ci], errors='coerce')
                                if pd.notna(val):
                                    numerics.append(val)
                            for i, field in enumerate(financial_fields):
                                record[field] = numerics[i] if i < len(numerics) else None

                        all_funds.append(record)

    fund_df = pd.DataFrame(all_funds)
    log.info(f"\nFund summary total: {len(fund_df):,} rows")
    return fund_df


def process_appropriation_summary(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Appropriation Summary (UAN)")
    log.info("=" * 60)

    all_approp, seen = [], set()
    for search_dir in search_paths(raw_dir, "financial"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if fname in seen: continue
            if 'appropriation_summary' in fname.lower().replace(" ", "_").replace("-", "_") and 'supplemental' not in fname.lower():
                seen.add(fname)
                log.info(f"\nProcessing: {fname}")
                fy = get_fy(fname)
                df = pd.read_excel(filepath, sheet_name=0, header=None)

                current_fund, current_department = None, None
                dept_keywords = [
                    'Public Service and Programs', 'Collection Development and Processing',
                    'Facilities Operation and Maintenance', 'Information Services',
                    'Business Administration', 'Support Services', 'Library Services',
                    'Capital Outlay', 'Debt Service', 'Other Financing Uses',
                ]

                for idx, row in df.iterrows():
                    if idx < 2: continue
                    label = row.iloc[0]
                    if pd.isna(label): continue
                    label = str(label).strip()
                    if not label or label in ['nan', 'None']: continue

                    # UAN Excel sometimes packs fund code + department group +
                    # department name into a single cell with embedded newlines,
                    # e.g. "1000 - General\nLibrary Services\nPublic Service and Programs".
                    # Split on newlines and process each sub-line.
                    sub_lines = [sl.strip() for sl in label.split('\n') if sl.strip()]

                    # Track whether this cell contained a fund/dept header so
                    # we know whether to also try parsing it as a line item.
                    is_header_cell = False

                    for sl in sub_lines:
                        fund_match = re.match(r'^(\d{4})\s*-\s*(.+)', sl)
                        if fund_match:
                            current_fund = fund_match.group(1)
                            is_header_cell = True
                            continue

                        if any(kw.lower() == sl.lower() for kw in dept_keywords):
                            current_department = sl
                            is_header_cell = True
                            continue

                    # If the cell was purely header info (fund/dept), skip to next row
                    if is_header_cell:
                        continue

                    # Otherwise treat the first sub-line as the line-item label
                    line_item = sub_lines[0] if sub_lines else label

                    numerics = []
                    for col_idx in range(1, len(row)):
                        val = pd.to_numeric(row.iloc[col_idx], errors='coerce')
                        if pd.notna(val): numerics.append(val)

                    record = {
                        'fiscal_year': fy, 'fund': current_fund,
                        'department': current_department, 'line_item': line_item,
                        'source_file': fname,
                    }
                    col_map = ['encumbrance_adj', 'final_appropriation', 'total_appropriations',
                               'to_date_expenditures', 'ytd_expenditures', 'reserve_for_encumbrance',
                               'unencumbered_balance', 'ytd_pct_expenditures']
                    for i, val in enumerate(numerics):
                        if i < len(col_map): record[col_map[i]] = val
                    all_approp.append(record)

    approp_df = pd.DataFrame(all_approp)
    log.info(f"\nAppropriation summary total: {len(approp_df):,} rows")
    return approp_df


def _parse_ledger_header_columns(row):
    """
    Dynamically detect column positions from a ledger header row.

    PDF-to-Excel conversion spreads columns inconsistently (e.g., 'Vendor / Payee'
    might appear at column 8 in one section and column 9 in another). This function
    scans the header row and builds a mapping of field name -> column index.

    Header rows may be single-row ('Post Date', 'Transaction Date', ...) or
    split across two rows ('Post' on one line, 'Date' on the next). This function
    handles the single-row case; the two-row case is handled by the caller
    merging adjacent rows.
    """
    col_map = {}
    for col_idx, val in enumerate(row):
        if pd.isna(val):
            continue
        s = str(val).strip().replace('\n', ' ')
        s_lower = s.lower()

        if 'post' in s_lower and 'date' in s_lower:
            col_map['post_date'] = col_idx
        elif 'transaction' in s_lower and 'date' in s_lower:
            col_map['transaction_date'] = col_idx
        elif 'process' in s_lower and 'id' in s_lower:
            col_map['process_id'] = col_idx
        elif 'vendor' in s_lower or 'payee' in s_lower:
            col_map['vendor_payee'] = col_idx
        elif s_lower == 'purpose':
            col_map['purpose'] = col_idx
        elif 'po' in s_lower and ('bc' in s_lower or '/' in s):
            col_map['po_bc'] = col_idx
        elif 'payment' in s_lower or 'receipt' in s_lower:
            col_map['payment_receipt_number'] = col_idx
        elif s_lower == 'expenditure':
            col_map['expenditure'] = col_idx
        elif s_lower == 'debit':
            col_map['debit'] = col_idx
        elif s_lower == 'credit':
            col_map['credit'] = col_idx
        elif 'unencumbered' in s_lower or (s_lower == 'balance' and col_idx > 20):
            col_map['unencumbered_balance'] = col_idx

    return col_map


def process_appropriation_ledger(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Appropriation Ledger (UAN)")
    log.info("=" * 60)

    all_ledger, all_approp_headers = [], []

    # --- Try PDFs first ---
    if HAS_PDFPLUMBER:
        pdf_files = find_pdfs(raw_dir, "financial", "appropriation_ledger")
        if pdf_files:
            for filepath in pdf_files:
                fy = get_fy(os.path.basename(filepath))
                txns, hdrs = _parse_approp_ledger_pdf(filepath, fy)
                all_ledger.extend(txns)
                all_approp_headers.extend(hdrs)

    # --- Fall back to Excel if no PDF results ---
    if not all_ledger:
        if HAS_PDFPLUMBER:
            log.info("  No PDF files found, falling back to Excel")
        all_ledger, all_approp_headers = _process_approp_ledger_excel(raw_dir)

    ledger_df = pd.DataFrame(all_ledger)

    # Convert date strings to datetime if they came from PDF (MM/DD/YYYY format)
    for col in ['post_date', 'transaction_date']:
        if col in ledger_df.columns:
            ledger_df[col] = pd.to_datetime(ledger_df[col], errors='coerce')

    log.info(f"\nAppropriation ledger total: {len(ledger_df):,} rows")

    approp_headers_df = pd.DataFrame(all_approp_headers)
    log.info(f"Appropriation headers (budget lines per account): {len(approp_headers_df):,} rows")

    return ledger_df, approp_headers_df


def _process_approp_ledger_excel(raw_dir):
    """Excel-based fallback for appropriation ledger processing."""

    # Labels for the budget header lines that appear before each account's
    # transaction section.
    APPROP_HEADER_LABELS = [
        'Temporary Appropriation:',
        'Original Appropriation:',
        'Permanent Appropriation:',
        'Final Appropriation:',
        'Report Beginning Balance:',
        'Reserved for Encumbrance 12/31:',
        'Reserved for Encumbrance 12/31 Adjustment:',
    ]

    all_ledger, all_approp_headers, seen = [], [], set()
    for search_dir in search_paths(raw_dir, "financial"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if fname in seen: continue
            if 'appropriation_ledger' in fname.lower().replace(" ", "_").replace("-", "_"):
                seen.add(fname)
                log.info(f"\nProcessing: {fname}")
                fy = get_fy(fname)
                df = pd.read_excel(filepath, sheet_name=0, header=None)

                current_account_code, current_fund, current_account_name = None, None, None
                in_transactions = False
                col_map = {}  # dynamically detected column positions

                for idx, row in df.iterrows():
                    row_str = ' '.join(str(v) for v in row.values if pd.notna(v))

                    # --- Account Code header ---
                    if 'Account Code:' in row_str:
                        for val in row.values:
                            if pd.notna(val) and str(val).strip() != 'Account Code:':
                                code = str(val).strip()
                                if re.match(r'\d{4}-\d{3}-\d{3}', code):
                                    current_account_code = code
                        in_transactions = False
                        continue

                    # --- Fund header ---
                    if 'Fund:' in row_str and 'Reserved' not in row_str.split('Fund:')[0]:
                        for val in row.values:
                            if pd.notna(val) and 'Fund:' not in str(val) and 'Reserved' not in str(val):
                                fund_val = str(val).strip()
                                if fund_val and fund_val not in ['nan', 'None']:
                                    current_fund = fund_val
                        continue

                    # --- Account Name header ---
                    if 'Account Name:' in row_str:
                        for val in row.values:
                            if pd.notna(val) and 'Account Name:' not in str(val) and 'Reserved' not in str(val):
                                name_val = str(val).strip()
                                if name_val and name_val not in ['nan', 'None']:
                                    current_account_name = name_val
                        continue

                    # --- Appropriation header rows (budget amounts per account) ---
                    # UAN reports list Temporary/Original/Permanent/Final
                    # Appropriation amounts in the header block of each account
                    # section.  Capture these into a separate table.
                    matched_header = None
                    for lbl in APPROP_HEADER_LABELS:
                        if lbl in row_str:
                            matched_header = lbl
                            break
                    if matched_header is not None:
                        # Extract the dollar amount from the row (if present).
                        # The amount may sit in any column to the right of the label.
                        amount = None
                        for val in row.values:
                            if pd.isna(val):
                                continue
                            s = str(val).strip()
                            # Skip the label text itself
                            if s == matched_header or s == matched_header.rstrip(':'):
                                continue
                            parsed = pd.to_numeric(
                                s.replace('$', '').replace(',', ''),
                                errors='coerce',
                            )
                            if pd.notna(parsed):
                                amount = parsed
                                break
                        all_approp_headers.append({
                            'fiscal_year': fy,
                            'account_code': current_account_code,
                            'fund': current_fund,
                            'account_name': current_account_name,
                            'label': matched_header.rstrip(':'),
                            'amount': amount,
                            'source_file': fname,
                        })
                        continue

                    # --- Transaction header row (detect column positions) ---
                    # PDF conversion may split headers across two rows:
                    #   Row N:   'Post'  'Transaction'           'Payment / Receipt'   'Unencumbered'
                    #   Row N+1: 'Date'  'Date'  'Process ID'  'Vendor / Payee' ...   'Balance'
                    # Or keep them on one row:
                    #   'Post Date'  'Transaction Date'  'Process ID'  'Vendor / Payee' ...
                    if 'Vendor' in row_str and ('Post' in row_str or 'Date' in row_str):
                        new_map = _parse_ledger_header_columns(row)
                        if len(new_map) >= 3:
                            col_map = new_map
                            log.debug(f"  Row {idx}: Detected columns: {col_map}")
                        in_transactions = True
                        continue

                    # Also catch the second half of a split header
                    if not in_transactions and 'Date' in row_str and 'Vendor' in row_str and 'Process' in row_str:
                        new_map = _parse_ledger_header_columns(row)
                        if len(new_map) >= 3:
                            col_map = new_map
                            log.debug(f"  Row {idx}: Detected columns (split header): {col_map}")
                        in_transactions = True
                        continue

                    if not in_transactions:
                        continue

                    # --- Skip summary/total rows ---
                    if any(kw in row_str for kw in ['Report Total', 'Account Total',
                                                     'Account YTD Total',
                                                     'Beginning Balance', 'Account Code:',
                                                     'Fund:', 'Account Name:']):
                        if 'Account Code:' in row_str:
                            # New account section -- reset
                            for val in row.values:
                                if pd.notna(val) and str(val).strip() != 'Account Code:':
                                    code = str(val).strip()
                                    if re.match(r'\d{4}-\d{3}-\d{3}', code):
                                        current_account_code = code
                            in_transactions = False
                        continue

                    # --- Parse transaction row using dynamic column map ---
                    # Determine post_date column (fall back to col 0)
                    post_date_col = col_map.get('post_date', 0)
                    post_date = row.iloc[post_date_col] if post_date_col < len(row) else None
                    if pd.isna(post_date):
                        continue

                    record = {
                        'fiscal_year': fy,
                        'account_code': current_account_code,
                        'fund': current_fund,
                        'account_name': current_account_name,
                        'post_date': parse_dates_safe(pd.Series([post_date])).iloc[0],
                        'source_file': fname,
                    }

                    # Text fields -- use dynamic column positions
                    text_fields = ['transaction_date', 'process_id', 'vendor_payee',
                                   'purpose', 'po_bc', 'payment_receipt_number']
                    for field_name in text_fields:
                        col_idx = col_map.get(field_name)
                        if col_idx is not None and col_idx < len(row):
                            val = row.iloc[col_idx]
                            if field_name == 'transaction_date':
                                record[field_name] = parse_dates_safe(pd.Series([val])).iloc[0]
                            else:
                                record[field_name] = str(val).strip() if pd.notna(val) else None
                        else:
                            record[field_name] = None

                    # Numeric fields -- use dynamic column positions, fall back to
                    # scanning from right side of row if positions not mapped
                    numeric_fields = ['expenditure', 'debit', 'credit', 'unencumbered_balance']
                    has_numeric_cols = any(f in col_map for f in numeric_fields)

                    if has_numeric_cols:
                        for field_name in numeric_fields:
                            col_idx = col_map.get(field_name)
                            if col_idx is not None and col_idx < len(row):
                                record[field_name] = pd.to_numeric(row.iloc[col_idx], errors='coerce')
                            else:
                                record[field_name] = None
                    else:
                        # Fallback: scan from right for numeric values
                        nums_found = []
                        start_col = max(col_map.values()) + 1 if col_map else 7
                        for col_idx in range(len(row) - 1, start_col - 1, -1):
                            val = pd.to_numeric(row.iloc[col_idx], errors='coerce')
                            if pd.notna(val):
                                nums_found.insert(0, val)
                        for i, field_name in enumerate(numeric_fields):
                            record[field_name] = nums_found[i] if i < len(nums_found) else None

                    all_ledger.append(record)

                log.info(f"  Parsed {len([r for r in all_ledger if r['source_file'] == fname])} transactions from {fname}")

    ledger_df = pd.DataFrame(all_ledger)

    # Clean up junk rows from PDF-to-Excel conversion artifacts.
    # Only remove rows where the vendor_payee field is clearly a
    # mis-parsed column header label -- NOT legitimate transaction
    # descriptions like "Enter Permanent Appropriation".
    if not ledger_df.empty and 'vendor_payee' in ledger_df.columns:
        junk_patterns = [
            r'^Vendor\s*/?\s*Payee$',
            r'^Post\s*Date$',
            r'^Transaction\s*Date$',
            r'^Process\s*ID$',
            r'^Purpose$',
            r'^PO\s*/?\s*BC$',
            r'^Payment\s*/?\s*Receipt',
            r'^Expenditure$',
            r'^Debit$',
            r'^Credit$',
            r'^Unencumbered\s*Balance$',
            r'^Balance$',
            r'^Number$',
            r'^Date$',
            r'^nan$',
            r'^None$',
        ]
        junk_regex = '|'.join(junk_patterns)
        before_count = len(ledger_df)
        ledger_df = ledger_df[
            ledger_df['vendor_payee'].isna() |
            ~ledger_df['vendor_payee'].str.match(junk_regex, case=False, na=False)
        ].reset_index(drop=True)
        removed = before_count - len(ledger_df)
        if removed > 0:
            log.info(f"  Removed {removed} junk rows (PDF conversion artifacts)")

    # Secondary cleanup: remove summary/total rows that slipped past the
    # in-loop skip check (e.g. "Account Total:", "Account YTD Total:",
    # "Report Total:").  These can land in any text column depending on
    # the Excel layout.
    if not ledger_df.empty:
        total_pattern = r'Account\s*(YTD\s*)?Total|Report\s*Total'
        text_cols = ['vendor_payee', 'purpose', 'process_id']
        mask = pd.Series(False, index=ledger_df.index)
        for col in text_cols:
            if col in ledger_df.columns:
                mask = mask | ledger_df[col].str.contains(
                    total_pattern, case=False, na=False)
        if mask.any():
            ledger_df = ledger_df[~mask].reset_index(drop=True)
            log.info(f"  Removed {mask.sum()} summary/total rows from ledger")

    log.info(f"\nAppropriation ledger (Excel): {len(ledger_df):,} rows")
    log.info(f"Appropriation headers (Excel): {len(all_approp_headers):,} rows")

    # Convert DataFrame back to list of dicts for the wrapper
    return ledger_df.to_dict('records') if not ledger_df.empty else [], all_approp_headers


def process_appropriation_supplemental(raw_dir):
    log.info("\n" + "=" * 60)
    log.info("PROCESSING: Appropriation Supplemental (UAN)")
    log.info("=" * 60)

    all_supp, seen = [], set()
    for search_dir in search_paths(raw_dir, "financial"):
        for filepath in find_files(search_dir):
            fname = os.path.basename(filepath)
            if fname in seen: continue
            if 'appropriation_supplemental' in fname.lower().replace(" ", "_").replace("-", "_"):
                seen.add(fname)
                log.info(f"\nProcessing: {fname}")
                fy = get_fy(fname)
                df = safe_read_excel(filepath, sheet_name=0, header=0)
                if df.empty: continue
                df = clean_column_names(df)
                df = df.dropna(how='all')
                # UAN exports repeat the header row at page breaks;
                # remove any rows where the 'post_date' cell literally
                # says "Post Date" (or similar header text).
                if 'post_date' in df.columns:
                    df = df[~df['post_date'].astype(str).str.strip().str.lower().eq('post date')]
                for col in ['post_date', 'date']:
                    if col in df.columns: df[col] = parse_dates_safe(df[col])
                if 'amount' in df.columns:
                    df['amount'] = pd.to_numeric(df['amount'].astype(str).str.replace(r'[\$,]', '', regex=True), errors='coerce')
                df['fiscal_year'] = fy
                df['source_file'] = fname
                all_supp.append(df)

    supp_df = pd.concat(all_supp, ignore_index=True) if all_supp else pd.DataFrame()
    log.info(f"\nAppropriation supplemental total: {len(supp_df):,} rows")
    return supp_df


# ---------------------------------------------------------------------------
# Database Loading
# ---------------------------------------------------------------------------

def load_to_sqlite(db_path, datasets):
    log.info("\n" + "=" * 60)
    log.info(f"LOADING TO SQLITE: {db_path}")
    log.info("=" * 60)

    ensure_dir(os.path.dirname(db_path))
    conn = sqlite3.connect(db_path)

    for table_name, df in datasets.items():
        if df.empty:
            log.warning(f"  Skipping empty dataset: {table_name}")
            continue
        for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        log.info(f"  Loaded {table_name}: {len(df):,} rows, {len(df.columns)} columns")

    views = {
        'v_physical_circ_summary': """
            CREATE VIEW IF NOT EXISTS v_physical_circ_summary AS
            SELECT collection_category,
                COUNT(*) as total_items,
                SUM(lifetime_circ) as total_lifetime_circ,
                SUM(prev_year_circ) as total_prev_year_circ,
                SUM(ytd_circ) as total_ytd_circ,
                AVG(lifetime_circ) as avg_lifetime_circ,
                SUM(CASE WHEN item_status = 'In' THEN 1 ELSE 0 END) as active_items
            FROM physical_items GROUP BY collection_category
        """,
        'v_overdrive_cost_per_circ': """
            CREATE VIEW IF NOT EXISTS v_overdrive_cost_per_circ AS
            SELECT format_type, fiscal_year,
                COUNT(*) as total_titles,
                SUM(library_price) as total_spent,
                SUM(all_checkouts) as total_checkouts,
                CASE WHEN SUM(all_checkouts) > 0
                    THEN ROUND(SUM(library_price) / SUM(all_checkouts), 2)
                    ELSE NULL END as cost_per_circ,
                AVG(turnover_rate) as avg_turnover
            FROM overdrive_detail WHERE library_price IS NOT NULL
            GROUP BY format_type, fiscal_year
        """,
        'v_hoopla_cost_per_circ': """
            CREATE VIEW IF NOT EXISTS v_hoopla_cost_per_circ AS
            SELECT format, fiscal_year,
                SUM(circulations) as total_circs,
                SUM(cost) as total_cost,
                CASE WHEN SUM(circulations) > 0
                    THEN ROUND(SUM(cost) / SUM(circulations), 2)
                    ELSE NULL END as cost_per_circ
            FROM hoopla_summary WHERE format != 'total'
            GROUP BY format, fiscal_year
        """,
        'v_digital_vs_physical_monthly': """
            CREATE VIEW IF NOT EXISTS v_digital_vs_physical_monthly AS
            SELECT year, month, month_num, category, circulation_count,
                CASE
                    WHEN category LIKE '%Print%' OR category LIKE '%Book%'
                         OR category LIKE '%AV%' OR category LIKE '%Audio%'
                         OR category LIKE '%DVD%' OR category LIKE '%Blu%'
                    THEN 'physical'
                    WHEN category LIKE '%Digital%' OR category LIKE '%Ebook%'
                         OR category LIKE '%Eaudio%' OR category LIKE '%Emusic%'
                         OR category LIKE '%Evideo%' OR category LIKE '%Emag%'
                    THEN 'digital'
                    ELSE 'other'
                END as format_group
            FROM circulation_totals WHERE month != 'total'
        """,
    }

    for view_name, view_sql in views.items():
        try:
            conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            conn.execute(view_sql)
            log.info(f"  Created view: {view_name}")
        except Exception as e:
            log.error(f"  Failed to create view {view_name}: {e}")

    conn.commit()
    conn.close()


def export_csvs(csv_dir, datasets):
    log.info("\n" + "=" * 60)
    log.info(f"EXPORTING CSVs: {csv_dir}")
    log.info("=" * 60)
    ensure_dir(csv_dir)
    for table_name, df in datasets.items():
        if df.empty:
            log.warning(f"  Skipping empty dataset: {table_name}")
            continue
        filepath = os.path.join(csv_dir, f"{table_name}.csv")
        df.to_csv(filepath, index=False)
        log.info(f"  Exported {table_name}.csv: {len(df):,} rows")


def generate_etl_report(datasets, output_dir):
    report = {
        'etl_run_timestamp': datetime.now().isoformat(),
        'datasets': {},
        'total_records': 0,
    }
    for name, df in datasets.items():
        info = {'rows': len(df), 'columns': len(df.columns), 'column_names': list(df.columns)}
        if not df.empty:
            info['null_counts'] = df.isnull().sum().to_dict()
            info['dtypes'] = {col: str(dtype) for col, dtype in df.dtypes.items()}
        report['datasets'][name] = info
        report['total_records'] += len(df)

    report_path = os.path.join(output_dir, 'etl_run_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    log.info(f"\nETL Report: {report_path} | Total records: {report['total_records']:,}")
    return report


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(raw_dir, output_dir):
    log.info("=" * 60)
    log.info("WMPL CAPSTONE â€” ETL PIPELINE")
    log.info(f"Raw data:  {raw_dir}")
    log.info(f"Output:    {output_dir}")
    log.info(f"Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # Physical Collection
    physical_items, collection_keys = process_physical_items(raw_dir)
    circulation_totals = process_circulation_totals(raw_dir)

    # Digital Content
    overdrive_detail = process_overdrive_detail(raw_dir)
    overdrive_checkouts = process_overdrive_checkouts(raw_dir)
    overdrive_purchase_orders = process_overdrive_purchase_orders(raw_dir)
    hoopla_summary = process_hoopla_summary(raw_dir)
    hoopla_detail = process_hoopla_detail(raw_dir)
    kanopy = process_kanopy(raw_dir)
    digital_content_usage = process_digital_content_usage(raw_dir)

    # Financial
    fund_summary = process_fund_summary(raw_dir)
    appropriation_summary = process_appropriation_summary(raw_dir)
    appropriation_ledger, ledger_appropriations = process_appropriation_ledger(raw_dir)
    appropriation_supplemental = process_appropriation_supplemental(raw_dir)

    datasets = {
        'physical_items': physical_items,
        'collection_keys': collection_keys,
        'circulation_totals': circulation_totals,
        'overdrive_detail': overdrive_detail,
        'overdrive_checkouts': overdrive_checkouts,
        'overdrive_purchase_orders': overdrive_purchase_orders,
        'hoopla_summary': hoopla_summary,
        'hoopla_detail': hoopla_detail,
        'kanopy': kanopy,
        'digital_content_usage': digital_content_usage,
        'fund_summary': fund_summary,
        'appropriation_summary': appropriation_summary,
        'appropriation_ledger': appropriation_ledger,
        'ledger_appropriations': ledger_appropriations,
        'appropriation_supplemental': appropriation_supplemental,
    }

    load_to_sqlite(os.path.join(output_dir, "sqlite", "wmpl.db"), datasets)
    export_csvs(os.path.join(output_dir, "csv"), datasets)
    report = generate_etl_report(datasets, output_dir)

    log.info("\n" + "=" * 60)
    log.info("ETL PIPELINE COMPLETE")
    log.info(f"SQLite: {os.path.join(output_dir, 'sqlite', 'wmpl.db')}")
    log.info(f"CSVs:   {os.path.join(output_dir, 'csv')}")
    log.info("=" * 60)
    return datasets, report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WMPL Capstone ETL Pipeline")
    parser.add_argument("--raw-dir", default=os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw"))
    parser.add_argument("--output-dir", default=os.path.join(os.path.dirname(__file__), "..", "..", "data", "processed"))
    args = parser.parse_args()
    run_pipeline(os.path.abspath(args.raw_dir), os.path.abspath(args.output_dir))
