import openai
import os
import glob
import difflib
import shutil

# Setup
openai.api_key = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4")  # Fallback to gpt-3.5-turbo if needed

def get_fixed_code(original_code):
    prompt = f"""
You are an Apache Airflow expert.
The following code may use deprecated or incompatible syntax for Airflow 3.0.

Please:
- Analyze and rewrite it to work in Airflow 3.0.
- Replace any deprecated or broken patterns.
- Preserve formatting and structure.
- Output ONLY the fixed Python code.

```python
{original_code}
```"""

    response = openai.ChatCompletion.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )
    return response.choices[0].message.content.strip()

def save_diff(original, fixed, file_path):
    diff = difflib.unified_diff(
        original.splitlines(), fixed.splitlines(),
        fromfile='original', tofile='fixed',
        lineterm=""
    )
    diff_path = f"{file_path}.diff"
    with open(diff_path, "w", encoding="utf-8") as f:
        f.write("\n".join(diff))
    print(f"Diff saved to: {diff_path}")

def analyze_and_fix(file_path, output_dir, backup_dir):
    with open(file_path, "r", encoding="utf-8") as f:
        original_code = f.read()

    fixed_code = get_fixed_code(original_code)

    # Save fixed version
    fixed_file_path = os.path.join(output_dir, os.path.basename(file_path))
    with open(fixed_file_path, "w", encoding="utf-8") as f:
        f.write(fixed_code)

    # Save diff
    save_diff(original_code, fixed_code, fixed_file_path)

    # Backup original file
    shutil.copy2(file_path, os.path.join(backup_dir, os.path.basename(file_path)))

    # Replace original file with fixed code
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(fixed_code)

    print(f"[UPDATED] {file_path} ✅")

def main():
    output_dir = "ai_fixes"
    backup_dir = "original_backups"
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(backup_dir, exist_ok=True)

    paths = glob.glob("gcs/amGlGCSCleanup.py", recursive=True)

    if not paths:
        print("No Python files found in 'dags/' or 'plugins/'.")
        return

    for path in paths:
        print(f"Analyzing and fixing: {path}")
        try:
            analyze_and_fix(path, output_dir, backup_dir)
        except Exception as e:
            print(f"[ERROR] Failed to process {path}: {e}")

if __name__ == "__main__":
    main()
