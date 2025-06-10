# scripts/check_airflow_compatibility.py

import openai
import os
import glob

openai.api_key = os.getenv("OPENAI_API_KEY")

def analyze_code(file_path):
    with open(file_path, "r") as f:
        code = f.read()

    prompt = f"""
You are an Apache Airflow expert.
Analyze the following code for compatibility with Airflow 3.0.
List:
- Deprecated or removed features used
- Breaking changes
- Suggested fixes
```python
{code}
```"""

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
    )

    return response.choices[0].message.content

def main():
    report_dir = "ai_reports"
    os.makedirs(report_dir, exist_ok=True)

    for path in glob.glob("dags/**/*.py", recursive=True):
        print(f"Analyzing: {path}")
        result = analyze_code(path)
        with open(f"{report_dir}/{os.path.basename(path)}.report.txt", "w") as f:
            f.write(result)
        print(result)

if __name__ == "__main__":
    main()
