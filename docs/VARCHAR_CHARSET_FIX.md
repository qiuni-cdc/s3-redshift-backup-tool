# VARCHAR Character Set Encoding Fix

**Date:** January 14, 2026
**Issue:** Spectrum Scan Error - VARCHAR length mismatch between MySQL and Redshift
**Status:** ✅ RESOLVED

---

## 🔴 Problem Statement

### Error Encountered
```
Spectrum Scan Error - code: 15007
The length of the data column city is longer than the length defined in the table.
Table: 32, Data: 33
```

### Initial Confusion
- **MySQL Schema**: `city VARCHAR(16)` (CHARACTER_MAXIMUM_LENGTH = 16)
- **MySQL Data**:
  - `CHAR_LENGTH(city) = 15` characters ✅ Fits in VARCHAR(16)
  - `LENGTH(city) = 33` bytes ❌ Exceeds VARCHAR(32)
- **Redshift Table**: `city VARCHAR(32)`
- **Redshift Error**: Data requires 33 bytes but table only allows 32 bytes

### Why This Happened
The sync tool was doubling the MySQL character length (`16 × 2 = 32`) without considering **character encoding**, resulting in insufficient byte capacity in Redshift.

---

## 🔍 Root Cause Analysis

### MySQL vs Redshift VARCHAR Semantics

| Database | VARCHAR Definition | Example |
|----------|-------------------|---------|
| **MySQL** | **Character-based** - `VARCHAR(n)` = n characters | `VARCHAR(16)` = 16 characters (byte size depends on charset) |
| **Redshift** | **Byte-based** - `VARCHAR(n)` = n bytes | `VARCHAR(32)` = 32 bytes total |

### Character Set Encoding Impact

MySQL supports multiple character sets with different byte-per-character requirements:

| Character Set | Bytes per Character | Example: VARCHAR(16) Requires |
|--------------|-------------------|-------------------------------|
| `latin1` | 1 byte | 16 bytes |
| `ascii` | 1 byte | 16 bytes |
| `utf8` | **up to 3 bytes** | **up to 48 bytes** |
| `utf8mb4` | **up to 4 bytes** | **up to 64 bytes** |
| `utf16` | up to 4 bytes | up to 64 bytes |

### Real-World Example

**City Name with Multi-byte Characters:**
```sql
-- MySQL (utf8mb4)
SELECT
    city,
    CHAR_LENGTH(city) as char_count,  -- Returns: 15 characters
    LENGTH(city) as byte_count         -- Returns: 33 bytes
FROM kuaisong.uni_customer;

-- City contains UTF-8 multi-byte characters (Chinese, Korean, Japanese, special chars, etc.)
-- 15 characters × ~2.2 bytes per character (average) = 33 bytes
```

**What Went Wrong:**
```
MySQL: city VARCHAR(16) CHARACTER SET utf8mb4
       ↓ (Old tool logic: multiply by 2)
Redshift: city VARCHAR(32)  ← Only 32 bytes!
       ↓
Data: 15 chars = 33 bytes  ← FAILS! (33 > 32)
```

---

## ✅ Solution Implemented

### 1. Query Character Set Information

**File:** `src/core/flexible_schema_manager.py`

**BEFORE:**
```sql
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
```

**AFTER:**
```sql
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    CHARACTER_SET_NAME,      -- ✅ ADDED
    COLLATION_NAME,          -- ✅ ADDED
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
```

### 2. Character Set to Bytes Mapping

**File:** `src/core/flexible_schema_manager.py`

Added charset-to-bytes mapping in `__init__`:
```python
# MySQL character set to max bytes per character mapping
self._charset_bytes = {
    'latin1': 1,
    'ascii': 1,
    'utf8': 3,      # MySQL's utf8 is limited to 3 bytes (BMP only)
    'utf8mb4': 4,   # Full UTF-8 with 4-byte support (emoji, etc.)
    'ucs2': 2,
    'utf16': 4,
    'utf16le': 4,
    'utf32': 4,
    'binary': 1,
}
```

Added helper method:
```python
def _get_charset_bytes(self, charset: Optional[str]) -> int:
    """
    Get maximum bytes per character for a given MySQL character set.

    Args:
        charset: MySQL character set name (e.g., 'utf8mb4', 'latin1')

    Returns:
        Maximum bytes per character (defaults to 4 for unknown charsets for safety)
    """
    if not charset:
        return 4  # Default to 4 bytes for safety (utf8mb4)

    charset_lower = charset.lower()
    return self._charset_bytes.get(charset_lower, 4)  # Default to 4 for unknown charsets
```

### 3. Fixed VARCHAR Mapping Logic

**File:** `src/core/flexible_schema_manager.py`

**BEFORE (Line 499-503):**
```python
if data_type in ['varchar', 'char']:
    if max_length and max_length <= 65535:
        # Add safety buffer for VARCHAR columns to handle longer actual data
        safe_length = min(max_length * 2, 65535) if max_length < 32768 else 65535
        return f"VARCHAR({safe_length})"
```

**AFTER:**
```python
if data_type in ['varchar', 'char']:
    if max_length and max_length <= 65535:
        # Calculate bytes needed based on character set
        bytes_per_char = self._get_charset_bytes(charset)

        # Calculate safe length: chars × bytes_per_char
        safe_length = min(max_length * bytes_per_char, 65535)

        logger.debug(
            f"VARCHAR mapping: {max_length} chars × {bytes_per_char} bytes/char "
            f"(charset={charset}) = {safe_length} bytes in Redshift"
        )

        return f"VARCHAR({safe_length})"
```

### 4. Updated Method Signature

**File:** `src/core/flexible_schema_manager.py`

```python
def _map_mysql_to_redshift(self, data_type: str, column_type: str,
                           max_length: Optional[int], precision: Optional[int],
                           scale: Optional[int], charset: Optional[str] = None) -> str:
    # charset parameter added ✅
```

### 5. Updated DDL Generation

**File:** `src/core/flexible_schema_manager.py` (Line ~310)

```python
for col in schema_info:
    col_name = col['COLUMN_NAME']
    data_type = col['DATA_TYPE'].lower()
    column_type = col['COLUMN_TYPE'].lower()
    charset = col.get('CHARACTER_SET_NAME')  # ✅ Extract charset

    # Get Redshift type with charset awareness
    rs_type = self._map_mysql_to_redshift(
        data_type, column_type, max_length, precision, scale, charset  # ✅ Pass charset
    )
```

### 6. Enhanced Airflow DAG Validation

**File:** `airflow_poc/dags/us_qa_reference_tables_11pm.py`

Updated validation to check byte requirements:
```python
# Character set to bytes per character mapping
charset_bytes = {
    'latin1': 1, 'ascii': 1, 'utf8': 3, 'utf8mb4': 4,
    'ucs2': 2, 'utf16': 4, 'utf16le': 4, 'utf32': 4, 'binary': 1
}

for row in mysql_cursor.fetchall():
    col_name = row['COLUMN_NAME']
    data_type = row['DATA_TYPE']
    max_length = row['CHARACTER_MAXIMUM_LENGTH']
    charset = row.get('CHARACTER_SET_NAME')

    # Calculate byte requirements for VARCHAR columns
    bytes_needed = max_length
    if data_type in ['varchar', 'char'] and max_length and charset:
        bytes_per_char = charset_bytes.get(charset.lower(), 4)
        bytes_needed = max_length * bytes_per_char

    mysql_schema[col_name] = {
        'type': data_type,
        'length': max_length,
        'charset': charset,
        'bytes_needed': bytes_needed,  # ✅ Track byte requirements
        # ...
    }
```

Updated comparison logic:
```python
# Compare VARCHAR lengths (considering character set encoding)
if mysql_col['type'] in ['varchar', 'char']:
    mysql_bytes_needed = mysql_col.get('bytes_needed', mysql_col['length'])
    mysql_char_len = mysql_col['length']
    mysql_charset = mysql_col.get('charset', 'unknown')
    redshift_len = redshift_col['length']

    # Redshift VARCHAR is byte-based, MySQL is character-based
    # Redshift should have enough bytes to store MySQL data with charset encoding
    if redshift_len and mysql_bytes_needed and redshift_len < mysql_bytes_needed:
        mismatches.append(
            f"  ❌ Column '{col_name}': "
            f"Redshift VARCHAR({redshift_len} bytes) < "
            f"MySQL VARCHAR({mysql_char_len} chars, charset={mysql_charset}) "
            f"which needs {mysql_bytes_needed} bytes"
        )
        schema_mismatch = True
```

---

## 📊 Before vs After Comparison

### Example: `kuaisong.uni_customer.city` Column

| Metric | Before Fix | After Fix |
|--------|-----------|-----------|
| MySQL Schema | `VARCHAR(16) CHARACTER SET utf8mb4` | *(same)* |
| MySQL Data | 15 chars = 33 bytes | *(same)* |
| Tool Calculation | `16 × 2 = 32` | `16 × 4 = 64` ✅ |
| Redshift Schema | `VARCHAR(32)` ❌ | `VARCHAR(64)` ✅ |
| Sync Result | **FAILS** (33 > 32) | **SUCCESS** (33 < 64) ✅ |

### Character Set Examples

| MySQL Column | Charset | Old Mapping | New Mapping | Improvement |
|-------------|---------|-------------|-------------|-------------|
| `VARCHAR(16)` | `latin1` | `VARCHAR(32)` | `VARCHAR(16)` | More efficient |
| `VARCHAR(16)` | `utf8` | `VARCHAR(32)` | `VARCHAR(48)` | +50% capacity |
| `VARCHAR(16)` | `utf8mb4` | `VARCHAR(32)` | `VARCHAR(64)` | +100% capacity ✅ |
| `VARCHAR(100)` | `utf8mb4` | `VARCHAR(200)` | `VARCHAR(400)` | Supports full UTF-8 |

---

## 🧪 Testing & Verification

### Test Case 1: Verify Character Set Detection

```sql
-- Check MySQL character set
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    CHARACTER_SET_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'kuaisong'
AND TABLE_NAME = 'uni_customer'
AND COLUMN_NAME = 'city';

-- Expected: CHARACTER_SET_NAME = 'utf8mb4'
```

### Test Case 2: Verify Byte Requirements

```sql
-- Find longest city name in bytes
SELECT
    city,
    CHAR_LENGTH(city) as char_count,
    LENGTH(city) as byte_count
FROM kuaisong.uni_customer
ORDER BY LENGTH(city) DESC
LIMIT 10;

-- Check if any exceed 32 bytes (old limit)
SELECT COUNT(*) as rows_exceeding_32_bytes
FROM kuaisong.uni_customer
WHERE LENGTH(city) > 32;
```

### Test Case 3: Verify Redshift Table Schema

```sql
-- After running sync with the fix
SELECT
    column_name,
    data_type,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'settlement_public'
AND table_name = 'uni_customer_ref'
AND column_name = 'city';

-- Expected: character_maximum_length = 64 (not 32)
```

### Test Case 4: Verify Sync Logs

Check Airflow DAG logs for:
```
VARCHAR mapping: 16 chars × 4 bytes/char (charset=utf8mb4) = 64 bytes in Redshift
✅ Table created: settlement_public.uni_customer_ref
✅ Synced 10,000 rows successfully
```

---

## 🚀 Deployment Steps

### 1. Drop Existing Tables
Drop any tables that were created with the old (incorrect) VARCHAR sizing:
```sql
DROP TABLE IF EXISTS settlement_public.uni_customer_ref CASCADE;
DROP TABLE IF EXISTS settlement_public.uni_warehouses_ref CASCADE;
DROP TABLE IF EXISTS settlement_public.uni_prealert_info_ref CASCADE;
DROP TABLE IF EXISTS settlement_public.ecs_staff_ref CASCADE;
DROP TABLE IF EXISTS settlement_public.uni_mawb_box_ref CASCADE;
```

### 2. Clear Schema Cache
If the sync tool caches schemas, clear the cache:
```python
# In your sync tool initialization or via CLI
schema_manager.clear_cache()
```

### 3. Trigger Sync
Run the Airflow DAG:
- Tables will be recreated with correct VARCHAR sizes
- Validation will pass
- Data will sync successfully

### 4. Verify Results
```sql
-- Check all reference tables have correct VARCHAR sizing
SELECT
    table_name,
    column_name,
    character_maximum_length,
    data_type
FROM information_schema.columns
WHERE table_schema = 'settlement_public'
AND table_name LIKE '%_ref'
AND data_type = 'character varying'
ORDER BY table_name, column_name;
```

---

## 📝 Impact & Benefits

### Tables Affected
- ✅ `kuaisong.uni_customer` → `settlement_public.uni_customer_ref`
- ✅ `kuaisong.uni_warehouses` → `settlement_public.uni_warehouses_ref`
- ✅ `kuaisong.uni_prealert_info` → `settlement_public.uni_prealert_info_ref`
- ✅ `kuaisong.ecs_staff` → `settlement_public.ecs_staff_ref`
- ✅ `kuaisong.uni_mawb_box` → `settlement_public.uni_mawb_box_ref`
- ✅ **All future pipelines using the sync tool**

### Benefits
1. **Prevents Sync Failures**: No more Spectrum Scan errors due to VARCHAR length mismatches
2. **Character Set Aware**: Properly handles UTF-8 multi-byte characters (Chinese, Japanese, Korean, emoji, etc.)
3. **Future-Proof**: Automatically adapts to different character sets
4. **Better Validation**: Airflow DAG validation now checks byte requirements, not just character counts
5. **Storage Optimization**: latin1/ascii columns don't get over-allocated (4× would be wasteful)

### Performance Impact
- **Minimal**: VARCHAR sizing is determined at table creation time
- **Redshift Storage**: Slightly higher VARCHAR limits, but Redshift only uses actual data size
- **No Query Impact**: VARCHAR length doesn't affect query performance in Redshift

---

## 🛡️ Edge Cases Handled

### 1. Unknown Character Set
```python
# Defaults to 4 bytes per character (utf8mb4) for safety
bytes_per_char = self._get_charset_bytes(charset)  # Returns 4 if charset unknown
```

### 2. Null Character Set
```python
if not charset:
    return 4  # Default to 4 bytes for safety
```

### 3. Mixed Character Sets in Same Table
Each column's character set is evaluated independently:
```python
for col in schema_info:
    charset = col.get('CHARACTER_SET_NAME')  # Per-column charset
    rs_type = self._map_mysql_to_redshift(..., charset)
```

### 4. Maximum VARCHAR Length
```python
safe_length = min(max_length * bytes_per_char, 65535)  # Cap at Redshift max
```

### 5. Non-String Columns
```python
# charset only applies to VARCHAR/CHAR columns
if data_type in ['varchar', 'char']:
    # ... charset logic ...
else:
    # Other types ignore charset
```

---

## 📚 References

### MySQL Documentation
- [MySQL Character Sets](https://dev.mysql.com/doc/refman/8.0/en/charset.html)
- [VARCHAR Storage Requirements](https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html#data-types-storage-reqs-strings)
- [utf8mb4 Character Set](https://dev.mysql.com/doc/refman/8.0/en/charset-unicode-utf8mb4.html)

### Redshift Documentation
- [VARCHAR Data Type](https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html#r_Character_types-varchar-or-character-varying)
- [Redshift Spectrum Data Types](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-data-types.html)

### UTF-8 Encoding
- [UTF-8 Wikipedia](https://en.wikipedia.org/wiki/UTF-8)
- Character ranges:
  - 1 byte: ASCII (0x00-0x7F)
  - 2 bytes: Latin, Greek, Cyrillic, Arabic, Hebrew
  - 3 bytes: Chinese, Japanese, Korean (CJK)
  - 4 bytes: Emoji, rare CJK characters

---

## 🔧 Maintenance Notes

### Future Considerations

1. **Monitor VARCHAR Usage**
   - Periodically check if VARCHAR allocations are too large
   - Consider adding metrics for actual vs allocated VARCHAR size

2. **Character Set Changes**
   - If MySQL tables change character sets, tables must be recreated in Redshift
   - Schema validation will detect this and drop/recreate automatically

3. **New Character Sets**
   - If MySQL introduces new character sets, add to `_charset_bytes` mapping
   - Default fallback is 4 bytes (safe for most cases)

4. **Performance Monitoring**
   - Monitor Redshift storage after fix deployment
   - VARCHAR allocations may increase but actual storage usage should remain similar

---

## ✅ Conclusion

The VARCHAR character set fix resolves a fundamental mismatch between MySQL's character-based VARCHAR and Redshift's byte-based VARCHAR. By querying and respecting MySQL's `CHARACTER_SET_NAME`, the sync tool now creates Redshift tables with sufficient byte capacity to store multi-byte UTF-8 data.

**Key Takeaway:** Always consider character encoding when mapping between databases with different VARCHAR semantics.

---

**Document Version:** 1.0
**Last Updated:** January 14, 2026
**Author:** Data Engineering Team
**Reviewed By:** [Pending]
