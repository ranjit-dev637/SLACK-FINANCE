with open('main.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    s = line.strip()
    if 'PROPERTIES' in s:
        print(f"{i+1}: {s}")
