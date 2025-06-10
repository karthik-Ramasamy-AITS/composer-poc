import os
import glob
import difflib
import shutil
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1")  # Default model that supports tools

def fix_code_with_code_ai(filename, original_code):
    prompt = f"""
You are an Apache Airflow expert.
Analyze the following code for compatibility with Airflow 3.0.0
List:.
- Deprecated or removed features used
- Breaking changes
- Suggested fixes

Here is the code from {filename}:
```python
{original_code}
```"""
    response = openai.ChatCompletion.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1
        )
    return response.choices[0].message.content.strip()

def show_diff_side_by_side(original, fixed):
    original_lines = original.splitlines()
    fixed_lines = fixed.splitlines()
    diff = difflib.unified_diff(original_lines, fixed_lines, lineterm="")
    print("\n".join(diff))

def ask_user_confirmation(filename):
    while True:
      choice = input(f"\nApply these changes to {filename}? (y/n): ").strip().lower()
      if choice in ["y", "n"]:
        return choice == "y"
      print("Please enter 'y' or 'n'.")  


def process_file(file_path, fixed_dir, backup_dir):
    with open(file_path, "r", encoding="utf-8") as f:
        original_code = f.read()
    try:
        fixed_code = fix_code_with_code_ai(file_path, original_code)
    except Exception as e:
        print(f"[ERROR] Failed to process {file_path}: {e}")
        return

    if fixed_code == original_code:
        print(f"[SKIPPED] No changes needed for {file_path}")
        return

    print(f"\n🔧 Suggested fix for {file_path}:\n")
    show_diff_side_by_side(original_code, fixed_code)

    if ask_user_confirmation(os.path.basename(file_path)):
        # Save diff for audit
        diff_path = os.path.join(fixed_dir, os.path.basename(file_path)) + ".diff"
        with open(diff_path, "w", encoding="utf-8") as f:
            diff = difflib.unified_diff(
                original_code.splitlines(), fixed_code.splitlines(),
                fromfile="original", tofile="fixed", lineterm=""
            )
            f.write("\n".join(diff))

        # Save fixed file
        fixed_path = os.path.join(fixed_dir, os.path.basename(file_path))
        with open(fixed_path, "w", encoding="utf-8") as f:
            f.write(fixed_code)

        # Backup and overwrite original
        shutil.copy2(file_path, os.path.join(backup_dir, os.path.basename(file_path)))
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(fixed_code)

        print(f"[✅ FIXED] {file_path}")
    else:
        print(f"[SKIPPED] No changes applied to {file_path}")

def main():
    print("----started-------")
    fixed_dir = "ai_fixes"
    backup_dir = "original_backups"
    os.makedirs(fixed_dir, exist_ok=True)
    os.makedirs(backup_dir, exist_ok=True)
    paths = glob.glob("gcs/amGlGCSCleanup.py", recursive=True)
    if not paths:
        print("No Python files found in 'dags/' or 'plugins/'.")
        return

    for path in paths:
        print(f"\n🔍 Reviewing: {path}")
        process_file(path, fixed_dir, backup_dir)

if __name__ == "__main__":
    main()

