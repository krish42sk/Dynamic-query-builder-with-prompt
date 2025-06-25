from sqlalchemy import create_engine, MetaData, Table, text
import pandas as pd
import re
import os
import json
import datetime
import platform
import subprocess
import openai

# ─────────────── DB CONFIG ─────────────── #
databaseName = 'RFDB_Server'
username = '15235'
password = 'naWqSIE~Ak.i'
hostName = '192.168.12.35'
port = '5432'
applicationName = 'vscode'

DATABASE_URL = (
    f"postgresql+psycopg2://{username}:{password}@{hostName}:{port}/{databaseName}"
    f"?application_name={applicationName.replace(' ', '%20')}"
)

engine = create_engine(DATABASE_URL)
metadata = MetaData()
DOWNLOADS_FOLDER = os.path.join(os.path.expanduser("~"), "Downloads")
MEMORY_FILE = "prompt_memory.json"
LOG_FILE = "query_log.jsonl"

# ─────────────── MEMORY ─────────────── #
current_context = {"schema": None, "tablename": None}
memory_per_table = {}
column_usage = {}

openai.api_key = os.getenv("OPENAI_API_KEY")

# ─────────────── MEMORY FUNCTIONS ─────────────── #
def save_memory():
    with open(MEMORY_FILE, "w") as f:
        json.dump({
            "current_context": current_context,
            "memory_per_table": memory_per_table,
            "column_usage": column_usage
        }, f)

def load_memory():
    global current_context, memory_per_table, column_usage
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE) as f:
            data = json.load(f)
            current_context.update(data.get("current_context", {}))
            memory_per_table.update(data.get("memory_per_table", {}))
            column_usage.update(data.get("column_usage", {}))

# ─────────────── LOGGING FUNCTION ─────────────── #
def log_interaction(prompt, prompt_type="structured", sql=None):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps({
            "timestamp": datetime.datetime.now().isoformat(),
            "type": prompt_type,
            "prompt": prompt,
            "sql": sql,
            "schema": current_context.get("schema"),
            "table": current_context.get("tablename")
        }) + "\n")

# ─────────────── OPENAI FUNCTION ─────────────── #
def nl_to_sql(natural_language_prompt, schema_hint=None):
    messages = [
        {"role": "system", "content": "You are a helpful assistant that converts natural language to SQL for PostgreSQL. Only return SQL."},
        {"role": "user", "content": natural_language_prompt}
    ]
    if schema_hint:
        messages.append({"role": "system", "content": f"Schema: {schema_hint}"})

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=messages,
            temperature=0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ OpenAI error: {e}")
        return None

# ─────────────── PARSE & EXECUTE ─────────────── #
def is_raw_sql(prompt):
    return prompt.strip().lower().startswith("select")

def parse_prompt(prompt):
    prompt = prompt.replace(" ", "")
    updates = dict(re.findall(r"(\w+):([^:,]+)", prompt))

    if "schema" in updates:
        current_context["schema"] = updates["schema"]
    if "tablename" in updates:
        current_context["tablename"] = updates["tablename"]

    if not current_context["schema"] or not current_context["tablename"]:
        return None, None, None, None, None, None

    key = f"{current_context['schema']}.{current_context['tablename']}"
    memory_per_table.setdefault(key, {"columns": "all", "condition": None, "order": None, "limit": None})

    for field in ("columns", "condition", "order", "limit"):
        if field in updates:
            memory_per_table[key][field] = updates[field]

    save_memory()
    return (
        current_context["schema"],
        current_context["tablename"],
        memory_per_table[key]["columns"],
        memory_per_table[key]["condition"],
        memory_per_table[key]["order"],
        memory_per_table[key]["limit"]
    )

def build_and_run_query(schema, tablename, columns, condition, order_by, limit):
    try:
        table = Table(tablename, metadata, autoload_with=engine, schema=schema)
        key = f"{schema}.{tablename}"
        if not columns:
            print("❌ No columns specified.")
            return

        selected_columns = table.c if columns.lower() == "all" else [table.c[col.strip()] for col in columns.split(",") if col.strip() in table.c]
        used_columns = [col.name for col in selected_columns] if columns.lower() == "all" else columns.split(",")

        if key not in column_usage:
            column_usage[key] = {}
        for col in used_columns:
            column_usage[key][col] = column_usage[key].get(col, 0) + 1

        stmt = table.select().with_only_columns(selected_columns)

        if condition:
            stmt = stmt.where(text(condition))
        if order_by:
            stmt = stmt.order_by(text(order_by))
        if limit:
            try:
                stmt = stmt.limit(int(limit))
            except ValueError:
                print("⚠️ Invalid limit, skipping.")

        with engine.connect() as conn:
            result = conn.execute(stmt)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            save_to_csv(df)

    except Exception as e:
        print(f"❌ Query error: {e}")


def run_raw_sql(query):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            save_to_csv(df)
    except Exception as e:
        print(f"❌ SQL error: {e}")

# ─────────────── SAVE & OPEN CSV ─────────────── #
def save_to_csv(df):
    if df.empty:
        print("⚠️ No rows returned.")
        return

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(DOWNLOADS_FOLDER, f"query_result_{current_context['tablename']}_{timestamp}.csv")
    df.to_csv(filename, index=False)
    print(f"✅ Saved: {filename}")
    open_csv(filename)

def open_csv(filepath):
    try:
        if platform.system() == "Windows":
            os.startfile(filepath)
        elif platform.system() == "Darwin":
            subprocess.call(["open", filepath])
        else:
            subprocess.call(["xdg-open", filepath])
    except Exception as e:
        print(f"⚠️ Cannot open CSV: {e}")

# ─────────────── USER HELP ─────────────── #
def print_help():
    print("""
📘 Commands:
  schema:public, tablename:employees, column:all
  column:name,email
  condition:city='Chennai'
  order:salary desc
  limit:50

  ask: natural language prompt
  /reset     → reset memory
  /memory    → show context
  /log       → show recent prompts
  /analytics → column usage stats
  /help      → this help
""")
    
def carve_function_into_file(func_name):
    func_defs = {
        "print_table_columns": '''
def print_table_columns(schema, tablename):
    from sqlalchemy import MetaData, Table, create_engine, text
    import pandas as pd
    import os
    metadata = MetaData()
    from __main__ import engine
    try:
        table = Table(tablename, metadata, autoload_with=engine, schema=schema)
        print(f"📋 Columns in {schema}.{tablename}:")
        for col in table.columns:
            print(f" - {col.name}")
    except Exception as e:
        print(f"❌ Could not load columns: {e}")
''',
        # Add other dynamic functions as needed
    }

    if func_name not in func_defs:
        print(f"❌ No template for function: {func_name}")
        return

    with open("dynamic_extensions.py", "a") as f:
        f.write("\n" + func_defs[func_name].strip() + "\n")

    print(f"🛠️ Auto-carved function: {func_name} → dynamic_extensions.py")


# ─────────────── MAIN LOOP ─────────────── #
def main():
    load_memory()
    print("🤖 AI SQL Assistant — Natural & Structured Prompting + Learning")
    print_help()

    while True:
        try:
            user_input = input("🧾 Prompt > ").strip()
            if not user_input:
                continue

            if user_input.lower() == "/reset":
                current_context.update({"schema": None, "tablename": None})
                memory_per_table.clear()
                column_usage.clear()
                if os.path.exists(MEMORY_FILE):
                    os.remove(MEMORY_FILE)
                print("🔁 Memory reset.")

            elif user_input.lower() == "/memory":
                print(f"Schema: {current_context['schema']}")
                print(f"Table : {current_context['tablename']}")
                key = f"{current_context['schema']}.{current_context['tablename']}"
                print(f"Memory: {memory_per_table.get(key, {})}")

            elif user_input.lower() == "/log":
                if not os.path.exists(LOG_FILE):
                    print("🗃️ No logs yet.")
                else:
                    with open(LOG_FILE) as f:
                        for line in f.readlines()[-5:]:
                            entry = json.loads(line)
                            print(f"[{entry['timestamp']}] {entry['type']} → {entry['prompt']}")

            elif user_input.lower() == "/analytics":
                print("📊 Column Usage Summary:")
                for table, usage in column_usage.items():
                    print(f"\n📁 {table}")
                    for col, count in sorted(usage.items(), key=lambda x: x[1], reverse=True):
                        print(f"  {col}: {count}")

            elif user_input.lower() == "/help":
                print_help()

            elif user_input.lower().startswith("ask:"):
                natural_prompt = user_input[4:].strip()
                schema = current_context.get("schema")
                table = current_context.get("tablename")
                if not schema or not table:
                    print("❌ Set schema and table first.")
                    continue

                try:
                    table_obj = Table(table, metadata, autoload_with=engine, schema=schema)
                    col_names = ", ".join([c.name for c in table_obj.columns])
                    schema_hint = f"{table}({col_names})"
                    sql = nl_to_sql(natural_prompt, schema_hint)
                    if sql:
                        print(f"🤖 SQL: {sql}")
                        log_interaction(natural_prompt, prompt_type="natural", sql=sql)
                        run_raw_sql(sql)
                except Exception as e:
                    print(f"❌ Error getting schema or generating SQL: {e}")
            elif re.match(r"(list|show|print)\\s+all\\s+column(s)?\\s+(of|from)?\\s*(\\w+)", user_input.lower()):
                match = re.match(r"(list|show|print)\\s+all\\s+column(s)?\\s+(of|from)?\\s*(\\w+)", user_input.lower())
                tablename = match.group(4)
                schema = current_context.get("schema") or input("📌 Enter schema name (e.g., public): ").strip()
                current_context["schema"] = schema
                current_context["tablename"] = tablename

                # Check if function already exists
                if not hasattr(__builtins__, "print_table_columns"):
                    carve_function_into_file("print_table_columns")

                from dynamic_extensions import print_table_columns
                print_table_columns(schema, tablename)
                continue

            elif user_input.lower().startswith("schema:"):
                schema_name = user_input[7:].strip()
                current_context["schema"] = schema_name
                print(f"📌 Schema set to: {schema_name}")

            elif user_input.lower().startswith("tablename:"):
                table_name = user_input[10:].strip()
                current_context["tablename"] = table_name
                print(f"📌 Table set to: {table_name}")
            elif is_raw_sql(user_input):
                log_interaction(user_input, prompt_type="raw")
                run_raw_sql(user_input)
            else:
                parsed = parse_prompt(user_input)
                if not parsed:
                    print("❌ Incomplete prompt. Set schema and table.")
                else:
                    log_interaction(user_input, prompt_type="structured")
                    build_and_run_query(*parsed)

        except KeyboardInterrupt:
            print("\n👋 Exiting.")
            break

if __name__ == "__main__":
    main()
