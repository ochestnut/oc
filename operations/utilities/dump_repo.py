#!/usr/bin/env python3
"""
dump_repo.py - Dump a directory's structure and file contents to a single text file.
Place in ~/Desktop/jst/ and run from anywhere.

Usage:
  python dump_repo.py                          # dumps all of jst/
  python dump_repo.py umm/scripts_analytics    # dumps a subdirectory
  python dump_repo.py umm/scripts_analytics out.txt  # custom output file
"""
import os
import sys
import json
import datetime

SKIP_DIRS = {'.git', '__pycache__', '.ipynb_checkpoints', 'node_modules', '.venv', 'venv'}
SKIP_EXTS = {'.pyc', '.pyo', '.so', '.o', '.gz', '.zip', '.png', '.jpg', '.jpeg', '.ico', '.pdf'}
MAX_FILE_BYTES = 100_000

JST_ROOT = os.path.dirname(os.path.abspath(__file__))


def dump(root, out):
    out.write(f"=== DIRECTORY TREE: {root} ===\n\n")
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in sorted(dirnames) if d not in SKIP_DIRS]
        depth = os.path.relpath(dirpath, root).count(os.sep)
        if dirpath != root:
            indent = '    ' * depth
            out.write(f"{'    ' * (depth - 1)}{os.path.basename(dirpath)}/\n".rjust(len(f"{'    ' * (depth - 1)}{os.path.basename(dirpath)}/\n")))
        else:
            indent = ''
            out.write(f"{os.path.basename(root)}/\n")
        for f in sorted(filenames):
            out.write(f"{'    ' * (depth + (0 if dirpath == root else 0))}{f}\n")

    out.write("\n\n=== FILE CONTENTS ===\n")
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in sorted(dirnames) if d not in SKIP_DIRS]
        for fname in sorted(filenames):
            if any(fname.endswith(ext) for ext in SKIP_EXTS):
                continue
            fpath = os.path.join(dirpath, fname)
            rel = os.path.relpath(fpath, root)
            out.write(f"\n\n{'=' * 60}\n")
            out.write(f"FILE: {rel}\n")
            out.write(f"{'=' * 60}\n")
            size = os.path.getsize(fpath)
            if size > MAX_FILE_BYTES:
                out.write(f"[SKIPPED: file too large ({size:,} bytes)]\n")
                continue
            try:
                with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                    out.write(f.read())
            except Exception as e:
                out.write(f"[ERROR reading file: {e}]\n")

def read_notebook(fpath):
    with open(fpath, 'r', encoding='utf-8') as f:
        nb = json.load(f)
    lines = []
    for cell in nb.get('cells', []):
        cell_type = cell.get('cell_type', '')
        source = ''.join(cell.get('source', []))
        lines.append(f"# [{cell_type}]\n{source}")
    return '\n\n'.join(lines)


def build_tree(root, prefix=''):
    lines = []
    entries = sorted(os.scandir(root), key=lambda e: (e.is_file(), e.name))
    entries = [e for e in entries if not (e.is_dir() and e.name in SKIP_DIRS)]
    for i, entry in enumerate(entries):
        connector = '└── ' if i == len(entries) - 1 else '├── '
        lines.append(f"{prefix}{connector}{entry.name}{'/' if entry.is_dir() else ''}")
        if entry.is_dir():
            extension = '    ' if i == len(entries) - 1 else '│   '
            lines.extend(build_tree(entry.path, prefix + extension))
    return lines


def dump_v2(root, out):
    out.write(f"=== DIRECTORY TREE: {root} ===\n\n")
    out.write(f"{os.path.basename(root)}/\n")
    out.write('\n'.join(build_tree(root)))
    out.write("\n\n\n=== FILE CONTENTS ===\n")
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in sorted(dirnames) if d not in SKIP_DIRS]
        for fname in sorted(filenames):
            if any(fname.endswith(ext) for ext in SKIP_EXTS):
                continue
            fpath = os.path.join(dirpath, fname)
            rel = os.path.relpath(fpath, root)
            out.write(f"\n\n{'=' * 60}\n")
            out.write(f"FILE: {rel}\n")
            out.write(f"{'=' * 60}\n")
            size = os.path.getsize(fpath)
            if size > MAX_FILE_BYTES and not fname.endswith('.ipynb'):
                out.write(f"[SKIPPED: file too large ({size:,} bytes)]\n")
                continue
            try:
                if fname.endswith('.ipynb'):
                    out.write(read_notebook(fpath))
                else:
                    with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                        out.write(f.read())
            except Exception as e:
                out.write(f"[ERROR reading file: {e}]\n")


if __name__ == '__main__':
    rel = sys.argv[1] if len(sys.argv) > 1 else '.'
    root = JST_ROOT if rel == '.' else os.path.join(JST_ROOT, rel)
    
    date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    dirname = rel.replace('/', '_').replace('.', 'jst') if rel != '.' else 'jst'
    outfile = sys.argv[2] if len(sys.argv) > 2 else os.path.join(JST_ROOT, f'dump_{dirname}_{date}.txt')

    if not os.path.isdir(root):
        print(f"Error: '{root}' is not a valid directory")
        sys.exit(1)

    with open(outfile, 'w', encoding='utf-8') as out:
        dump_v2(root, out)

    size_kb = os.path.getsize(outfile) / 1024
    print(f"Written to {outfile} ({size_kb:.1f} KB)")