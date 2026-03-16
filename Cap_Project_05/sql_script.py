"""
sql_script.py
Single script to:
- create database & tables
- read Excel sheet
- extract unique subject names from the subject full-name columns (subject1..subject11)
- populate marks_subject, marks_subjectoffering, marks_branch, marks_regulation, marks_section, marks_examtype
- insert students (with id auto-increment PK) and marks (marks_marks with subject_id)
Note:
- marks_marks.student_id stores roll_number string (no FK constraint on it) because students repeat multiple rows.
- Adjust CSV_FILE_PATH and DB_CONFIG before running.
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error

# ========== CONFIG ==========
CSV_FILE_PATH = r"C:/Users/raagc/Downloads/AAC Core Semester Marks Details.xlsx"  # change if needed
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "admin@123",
    "database": "griet_results"   # will be created if not exists
}

# ========== HELPERS ==========

def to_roman(num):
    roman_map = {1:"I",2:"II",3:"III",4:"IV",5:"V",6:"VI",7:"VII",8:"VIII",9:"IX",10:"X"}
    try:
        return roman_map[int(num)]
    except:
        return str(num) if pd.notna(num) else None

def connect_db():
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"]
        )
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}`")
        conn.database = DB_CONFIG["database"]
        print("✅ Connected to MySQL and database ready.")
        cur.close()
        return conn
    except Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

# ========== CREATE TABLES ==========
def create_tables(connection):
    cur = connection.cursor()
    # Use strict schema that matches your sample (and comments above)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_regulation (
        id INT AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(20),
        passout_year INT,
        UNIQUE (code, passout_year)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_branch (
        id INT AUTO_INCREMENT PRIMARY KEY,
        code VARCHAR(20) UNIQUE,
        name VARCHAR(200)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_section (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(20) UNIQUE
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_subject (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) UNIQUE
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_examtype (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) UNIQUE
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_subjectoffering (
        id INT AUTO_INCREMENT PRIMARY KEY,
        subject_id INT,
        regulation_id INT,
        branch_id INT,
        semester VARCHAR(20),
        FOREIGN KEY (subject_id) REFERENCES marks_subject(id),
        FOREIGN KEY (regulation_id) REFERENCES marks_regulation(id),
        FOREIGN KEY (branch_id) REFERENCES marks_branch(id)
    )
    """)
    # Student: use id auto_increment as PK (so duplicate roll_number rows allowed)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_student (
        id INT AUTO_INCREMENT PRIMARY KEY,
        roll_number VARCHAR(50),
        name VARCHAR(200),
        year VARCHAR(20),
        semester VARCHAR(20),
        regulation_id INT,
        branch_id INT,
        section_id INT,
        phone_number VARCHAR(30),
        FOREIGN KEY (regulation_id) REFERENCES marks_regulation(id),
        FOREIGN KEY (branch_id) REFERENCES marks_branch(id),
        FOREIGN KEY (section_id) REFERENCES marks_section(id)
    )
    """)
    # marks_marks: student_id stored as roll_number string (not FK), subject_id referenced, exam_type_id referenced
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marks_marks (
        id INT AUTO_INCREMENT PRIMARY KEY,
        student_id VARCHAR(50),
        subject_id INT,
        exam_type_id INT,
        marks_obtained INT,
        max_marks INT DEFAULT 30,
        FOREIGN KEY (subject_id) REFERENCES marks_subject(id),
        FOREIGN KEY (exam_type_id) REFERENCES marks_examtype(id)
        -- note: we do NOT add FK on student_id because roll_number repeats in marks_student rows
    )
    """)
    connection.commit()
    cur.close()

# ========== UTILS for lookup/insert ==========
def get_id_by_columns(cur, table, where_clause, params):
    # Generic helper to fetch id by arbitrary where clause
    cur.execute(f"SELECT id FROM {table} WHERE {where_clause}", params)
    r = cur.fetchone()
    return r[0] if r else None

def get_or_create_regulation(cur, code, passout_year):
    # code may be None or empty; handle gracefully
    code_val = code if code not in (None, "", float("nan")) else None
    existing = None
    if code_val is None:
        # try match on passout_year only
        if passout_year is not None:
            existing = get_id_by_columns(cur, "marks_regulation", "passout_year=%s", (passout_year,))
    else:
        existing = get_id_by_columns(cur, "marks_regulation", "code=%s AND passout_year=%s", (code_val, passout_year))
    if existing:
        return existing
    cur.execute("INSERT INTO marks_regulation (code, passout_year) VALUES (%s,%s)", (code_val, passout_year))
    return cur.lastrowid

def get_or_create_branch(cur, code, name=None):
    # If spreadsheet provides short code (CSE) we'll use it in code; name optional
    code_val = code
    existing = None
    if code_val:
        existing = get_id_by_columns(cur, "marks_branch", "code=%s", (code_val,))
    if existing:
        # optionally update name if provided and NULL
        if name:
            cur.execute("UPDATE marks_branch SET name=%s WHERE id=%s AND (name IS NULL OR name='')", (name, existing))
        return existing
    cur.execute("INSERT INTO marks_branch (code, name) VALUES (%s,%s)", (code_val, name))
    return cur.lastrowid

def get_or_create_section(cur, name):
    if name is None or name == "":
        return None
    existing = get_id_by_columns(cur, "marks_section", "name=%s", (name,))
    if existing:
        return existing
    cur.execute("INSERT INTO marks_section (name) VALUES (%s)", (name,))
    return cur.lastrowid

def get_or_create_subject(cur, subject_name):
    if subject_name is None or str(subject_name).strip()=="":
        return None
    name = str(subject_name).strip()
    existing = get_id_by_columns(cur, "marks_subject", "name=%s", (name,))
    if existing:
        return existing
    cur.execute("INSERT INTO marks_subject (name) VALUES (%s)", (name,))
    return cur.lastrowid

def get_or_create_examtype(cur, name):
    existing = get_id_by_columns(cur, "marks_examtype", "name=%s", (name,))
    if existing:
        return existing
    cur.execute("INSERT INTO marks_examtype (name) VALUES (%s)", (name,))
    return cur.lastrowid

def get_or_create_subjectoffering(cur, subject_id, regulation_id, branch_id, semester):
    # check unique combination; if exists return id, else insert
    existing = get_id_by_columns(
        cur,
        "marks_subjectoffering",
        "subject_id=%s AND regulation_id=%s AND branch_id=%s AND semester=%s",
        (subject_id, regulation_id, branch_id, semester)
    )
    if existing:
        return existing
    cur.execute("""
        INSERT INTO marks_subjectoffering (subject_id, regulation_id, branch_id, semester)
        VALUES (%s,%s,%s,%s)
    """, (subject_id, regulation_id, branch_id, semester))
    return cur.lastrowid

# ========== MAIN DATA INSERTION ==========
def insert_excel_data(connection, df):
    cur = connection.cursor()

    # 1) Prepopulate exam types (keep order same as sample maybe)
    exam_types = ["Mid1", "Mid2", "Internal", "External"]
    for et in exam_types:
        get_or_create_examtype(cur, et)

    # 2) Prepopulate sections (if you want A..Z) - but we will insert only those present
    # We'll fetch unique sections from df if present:
    if "Section" in df.columns:
        for s in df["Section"].dropna().unique():
            get_or_create_section(cur, str(s).strip())

    # 3) Prepopulate branches using Branch column if present
    if "Branch" in df.columns:
        for b in df["Branch"].dropna().unique():
            bstr = str(b).strip()
            # if it's long name or short code: attempt to derive code as common 3-4 char uppercase token,
            # but per your instruction we'll keep short forms if present. If cell looks like "Computer Science and Engineering (CSE)",
            # try extract code in parens.
            code = bstr
            name = None
            # detect pattern like "Computer Science and Engineering (CSE)"
            if "(" in bstr and ")" in bstr:
                # take content inside last parentheses as code
                try:
                    inside = bstr[bstr.rfind("(")+1:bstr.rfind(")")]
                    if len(inside) <= 6:
                        code = inside.strip()
                        # name is whole before parentheses
                        name = bstr[:bstr.rfind("(")].strip()
                except:
                    code = bstr
            else:
                # if bstr looks short (<=5, no spaces), use as code; else try first token or first 3 uppercase letters
                if " " in bstr:
                    # keep as code shorter form if possible: e.g., "CSE" present? else code = first token
                    parts = bstr.split()
                    if len(bstr) <= 6:
                        code = bstr
                    else:
                        # attempt create short code by taking uppercase letters from words
                        code_chars = "".join([w[0].upper() for w in parts if w])
                        if 2 <= len(code_chars) <= 6:
                            code = code_chars
                        else:
                            code = bstr[:6].upper()
                    name = bstr
                else:
                    code = bstr
                    name = None
            get_or_create_branch(cur, code, name)

    # 4) Determine which columns are the subject full-name columns.
    # The user said "full name of the subject1 ... subject11" — we'll detect columns that contain 'subject' and 'Full' or start with 'Subject' etc.
    # But to be robust, we accept column names like: "Full Name of Subject1", "Subject 1 - Full Name", "Subject1", "Subject Name 1", etc.
    subj_col_candidates = []
    for col in df.columns:
        col_low = str(col).strip().lower()
        if "subject" in col_low and ("name" in col_low or "full" in col_low or any(ch.isdigit() for ch in col_low)):
            subj_col_candidates.append(col)
    # Fallback: if above fails, try any columns that look like 'Subject1'..'Subject11' directly
    if not subj_col_candidates:
        for i in range(1, 20):   # try up to 19 just in case
            name = f"Subject{i}"
            if name in df.columns:
                subj_col_candidates.append(name)

    # If user explicitly has columns named 'Full Name of Subject1'..'Full Name of Subject11' they will be included by above.
    # Now collect all unique subject names across these columns (strip blanks)
    unique_subjects = set()
    for col in subj_col_candidates:
        for v in df[col].dropna().unique():
            if str(v).strip() != "":
                unique_subjects.add(str(v).strip())

    # Insert these unique subjects into marks_subject ONLY
    # (this matches user's exact requirement)
    subject_map = {}  # name -> id
    for s in sorted(unique_subjects):  # sort for deterministic order
        sid = get_or_create_subject(cur, s)
        subject_map[s] = sid

    # 5) Iterate rows to insert regulations, students, marks, and subject offerings
    for _, row in df.iterrows():
        # --- Regulation ---
        raw_reg = row.get("GR Regulation - Passout Year") if "GR Regulation - Passout Year" in row.index else row.get("Regulation")
        reg_code, passout_year = None, None
        if pd.notna(raw_reg):
            raw = str(raw_reg).strip()
            if "-" in raw:
                parts = [p.strip() for p in raw.split("-")]
                if len(parts) >= 2:
                    reg_code = parts[0]
                    try:
                        passout_year = int(parts[1])
                    except:
                        passout_year = None
                else:
                    reg_code = raw
            else:
                # maybe it's like 'GR22 2026' or 'GR22/2026'
                tokens = raw.replace("/", " ").split()
                if len(tokens) >= 2 and tokens[-1].isdigit():
                    reg_code = " ".join(tokens[:-1])
                    try:
                        passout_year = int(tokens[-1])
                    except:
                        passout_year = None
                else:
                    # try to parse year digits
                    digits = ''.join([c for c in raw if c.isdigit()])
                    if len(digits) >= 4:
                        try:
                            passout_year = int(digits[:4])
                        except:
                            passout_year = None
                    reg_code = raw
        reg_id = get_or_create_regulation(cur, reg_code, passout_year)

        # --- Branch ---
        branch_name_cell = row.get("Branch") if "Branch" in row.index else None
        branch_code = None
        branch_name = None
        if pd.notna(branch_name_cell):
            bstr = str(branch_name_cell).strip()
            # if there is parentheses code like "Computer Science and Engineering (CSE)"
            if "(" in bstr and ")" in bstr:
                try:
                    inside = bstr[bstr.rfind("(")+1:bstr.rfind(")")]
                    branch_code = inside.strip()
                    branch_name = bstr[:bstr.rfind("(")].strip()
                except:
                    branch_code = bstr
                    branch_name = None
            else:
                # if it's short like "CSE"
                if len(bstr) <= 6 and bstr.isupper():
                    branch_code = bstr
                    branch_name = None
                else:
                    # keep bstr as name, make a code guess
                    branch_name = bstr
                    parts = bstr.split()
                    code_guess = "".join([w[0].upper() for w in parts if w])
                    branch_code = code_guess if code_guess else bstr[:6].upper()
        else:
            branch_code = None
            branch_name = None
        branch_id = get_or_create_branch(cur, branch_code, branch_name)

        # --- Section ---
        section_name = None
        if "Section" in df.columns and pd.notna(row.get("Section")):
            section_name = str(row.get("Section")).strip()
        section_id = get_or_create_section(cur, section_name) if section_name else None

        # --- Semester & Year ---
        semester = to_roman(row.get("Semester")) if "Semester" in row.index and pd.notna(row.get("Semester")) else None
        year = to_roman(row.get("Year")) if "Year" in row.index and pd.notna(row.get("Year")) else None

        # --- Student: insert new row (id auto increment). Keep roll_number as column (may repeat)
        roll_no = str(row.get("Roll Number","")).strip() if "Roll Number" in row.index else ""
        name = str(row.get("Name","")).strip() if "Name" in row.index else ""
        phone_number = str(row.get("Phone Number","")).strip() if "Phone Number" in row.index else None

        cur.execute("""
            INSERT INTO marks_student
            (roll_number,name,year,semester,regulation_id,branch_id,section_id,phone_number)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (roll_no,name,year,semester,reg_id,branch_id,section_id,phone_number))
        student_row_id = cur.lastrowid

        # --- For each subject column among the subject-full-name columns, insert marks and subject offering
        for col in subj_col_candidates:
            if col not in row.index:
                continue
            subj_full = row[col]
            if pd.isna(subj_full) or str(subj_full).strip()=="":
                continue
            subj_name = str(subj_full).strip()

            # only handle if this subject is among the unique_subjects we inserted earlier
            if subj_name not in subject_map:
                # (skip - user asked marks_subject should contain only unique values from those columns)
                continue
            subject_id = subject_map[subj_name]
            # create subject offering entry for (subject, regulation, branch, semester)
            # Some rows might be from different semester/regulation combos -> will create or reuse offering
            subj_offering_id = get_or_create_subjectoffering(cur, subject_id, reg_id, branch_id, semester)

            # Now determine the marks_numeric for this subject — depending on sheet you may have marks in another column
            # Some spreadsheets put the marks in a different column e.g. "Marks - Subject1" while subject name in "Full Name of Subject1".
            # We try: if column name itself is the mark (i.e., subject column directly contains numeric marks), use that.
            # But typical format is: subject column contains subject name and separate column like "Subject1 Marks" contains marks.
            # We'll attempt to find marks by searching sibling columns:
            marks_val = None
            # If the cell itself is numeric (rare if subject name), use it
            try:
                if isinstance(subj_full, (int, float)) and not isinstance(subj_full, str):
                    marks_val = int(subj_full)
            except:
                marks_val = None
            # If not numeric, look for sibling column patterns:
            # Example patterns: replace "Full Name" -> "Marks", or add "Marks" suffix, or "Subject1 Marks"
            numeric_candidates = []
            col_name = str(col)
            # common possibilities
            candidates = [
                col_name.replace("Full Name of ", "").strip() + " Marks",
                col_name + " Marks",
                col_name + " - Marks",
                col_name.replace("Full Name of ", "").strip(),
                col_name + "_marks",
                col_name + " Marks Obtained",
                col_name.replace("Full Name of ", "").strip() + " (Marks)",
            ]
            # plus try pattern: if column has digit like Subject1 then try 'Marks1' etc.
            import re
            digits = re.findall(r"\d+", col_name)
            if digits:
                d = digits[0]
                candidates.extend([f"Marks{d}", f"Mark{d}", f"Subject{d} Marks", f"Marks Subject{d}"])
            # Search the row for any of the candidate column names
            for c in candidates:
                if c in row.index:
                    val = row.get(c)
                    if pd.notna(val):
                        numeric_candidates.append(val)
            # As fallback: check for any numeric-like columns in the row (but cautious)
            if not numeric_candidates:
                # look for columns whose header contains 'mark' or 'score' or 'obtained'
                for cc in row.index:
                    cc_low = str(cc).lower()
                    if "mark" in cc_low or "score" in cc_low or "obtained" in cc_low:
                        val = row.get(cc)
                        if pd.notna(val):
                            numeric_candidates.append(val)
            # pick first numeric candidate (if any)
            chosen_mark = None
            for cand in numeric_candidates:
                try:
                    if isinstance(cand, str):
                        cand = cand.strip()
                    mv = int(float(cand))
                    chosen_mark = mv
                    break
                except:
                    continue
            # If still not found, we will not insert a marks row (safe)
            if chosen_mark is None:
                # possibly subject column itself includes "SUBJECT NAME (marks: 28)" - try extract digits
                if isinstance(subj_full, str):
                    found_digits = re.findall(r"\d+", subj_full)
                    if found_digits:
                        try:
                            chosen_mark = int(found_digits[-1])
                        except:
                            chosen_mark = None

            if chosen_mark is None:
                # skip inserting marks if we couldn't find marks numeric
                continue

            # cap / normalize marks: user expects maybe out of 30; script leaves as-is but you can adjust default max_marks
            try:
                marks_int = int(chosen_mark)
            except:
                marks_int = 0

            # Decide exam_type: simple default 'External' OR the script can detect a global ExamType column
            exam_type_id = get_or_create_examtype(cur, "External")
            # If there is a column named "Exam Type" use it
            if "Exam Type" in row.index and pd.notna(row.get("Exam Type")):
                exam_type_id = get_or_create_examtype(cur, str(row.get("Exam Type")).strip())

            # Insert into marks_marks with student_id as roll_no string and subject_id (not subject_offering_id)
            cur.execute("""
                INSERT INTO marks_marks (student_id, subject_id, exam_type_id, marks_obtained, max_marks)
                VALUES (%s,%s,%s,%s,%s)
            """, (roll_no, subject_id, exam_type_id, marks_int, 30))

    connection.commit()
    cur.close()

# ========== SCRIPT ENTRY POINT ==========
def main():
    print("Reading Excel:", CSV_FILE_PATH)
    try:
        df = pd.read_excel(CSV_FILE_PATH, dtype=object)
    except Exception as e:
        print("❌ Failed to read Excel file:", e)
        return

    conn = connect_db()
    if not conn:
        return

    create_tables(conn)
    print("📚 Tables created/checked.")

    insert_excel_data(conn, df)
    print("✅ Data imported successfully.")

    conn.close()

if __name__ == "__main__":
    main()
