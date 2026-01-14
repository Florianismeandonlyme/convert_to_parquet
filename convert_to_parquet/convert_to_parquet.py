#!/usr/bin/env python3
"""
批量将指定文件夹下的 csv/xlsx/dta 文件转为 Parquet（Hive 风格目录结构镜像）。

用法示例:
  python convert_to_parquet.py --input "C:/data/myfolder" --output "C:/data/parquet_out"
如果不传 `--input`，会弹出文件夹选择对话框（Windows）。

依赖: pandas, pyarrow, openpyxl, pyreadstat
"""
import argparse
import os
import sys
from typing import List


def choose_directory_dialog() -> str:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        print("没有可用的图形界面库，请通过 --input 指定目录。", file=sys.stderr)
        sys.exit(1)
    root = tk.Tk()
    root.withdraw()
    folder = filedialog.askdirectory()
    root.destroy()
    return folder


def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def write_df_to_parquet(df, out_path: str):
    # 使用 pandas to_parquet（pyarrow 引擎）
    df.to_parquet(out_path, engine="pyarrow", index=False)


def process_csv(path: str, out_dir: str, basename: str, chunk_size: int = 200_000):
    import pandas as pd
    file_size = os.path.getsize(path)
    if file_size > 50 * 1024 * 1024:  # >50MB 使用分块
        part = 0
        part_dir = os.path.join(out_dir, basename)
        ensure_dir(part_dir)
        for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False, encoding_errors='replace'):
            out_file = os.path.join(part_dir, f"part-{part:05d}.parquet")
            write_df_to_parquet(chunk, out_file)
            part += 1
        return [os.path.join(part_dir, f) for f in sorted(os.listdir(part_dir))]
    else:
        df = pd.read_csv(path, low_memory=False, encoding_errors='replace')
        out_file = os.path.join(out_dir, f"{basename}.parquet")
        write_df_to_parquet(df, out_file)
        return [out_file]


def process_excel(path: str, out_dir: str, basename: str):
    import pandas as pd
    # 只读取第一个 sheet（若需全部 sheet，可改为 sheet_name=None）
    df = pd.read_excel(path, sheet_name=0, engine='openpyxl')
    out_file = os.path.join(out_dir, f"{basename}.parquet")
    write_df_to_parquet(df, out_file)
    return [out_file]


def process_dta(path: str, out_dir: str, basename: str):
    import pandas as pd
    df = pd.read_stata(path)
    out_file = os.path.join(out_dir, f"{basename}.parquet")
    write_df_to_parquet(df, out_file)
    return [out_file]


def find_files(root: str, exts: List[str]) -> List[str]:
    matches = []
    for dirpath, dirnames, filenames in os.walk(root):
        for f in filenames:
            if os.path.splitext(f)[1].lower() in exts:
                matches.append(os.path.join(dirpath, f))
    return matches


def main():
    parser = argparse.ArgumentParser(description="批量将 csv/xlsx/dta 转为 Parquet（Hive 风格目录镜像）")
    parser.add_argument('--input', '-i', help='要处理的根目录')
    parser.add_argument('--output', '-o', help='Parquet 输出目录（默认：<input>/_parquet_out）')
    parser.add_argument('--delete', action='store_true', help='转换完成后自动删除原文件（慎用）')
    args = parser.parse_args()

    input_dir = args.input or choose_directory_dialog()
    if not input_dir:
        print('未选择输入目录，退出。')
        sys.exit(1)
    input_dir = os.path.abspath(input_dir)
    output_dir = args.output or os.path.join(input_dir, '_parquet_out')
    ensure_dir(output_dir)

    exts = {'.csv', '.xlsx', '.xls', '.dta'}
    files = find_files(input_dir, list(exts))
    if not files:
        print('在目录中未发现 csv/xlsx/dta 文件。')
        sys.exit(0)

    summary = []
    for f in files:
        rel_dir = os.path.relpath(os.path.dirname(f), input_dir)
        target_dir = os.path.join(output_dir, rel_dir)
        ensure_dir(target_dir)
        name = os.path.splitext(os.path.basename(f))[0]
        ext = os.path.splitext(f)[1].lower()
        try:
            if ext == '.csv':
                out_files = process_csv(f, target_dir, name)
            elif ext in ('.xlsx', '.xls'):
                out_files = process_excel(f, target_dir, name)
            elif ext == '.dta':
                out_files = process_dta(f, target_dir, name)
            else:
                out_files = []
            summary.append((f, out_files, None))
            print(f"已转换: {f} -> {len(out_files)} Parquet 文件")
        except Exception as e:
            print(f"转换失败: {f} 错误: {e}")
            summary.append((f, [], str(e)))

    # 完成后询问是否删除源文件（若用户未在命令行传 --delete，再次确认）
    if args.delete:
        confirm = 'y'
    else:
        confirm = input('是否删除原始文件？输入 y 确认，其他取消: ').strip().lower()

    if confirm == 'y':
        for orig, outs, err in summary:
            try:
                os.remove(orig)
            except Exception as e:
                print(f"删除失败: {orig} 错误: {e}")
        print('已尝试删除所有原文件（如有权限问题会显示错误）。')
    else:
        print('保留了原始文件。')

    print('\n转换摘要:')
    succ = sum(1 for s in summary if s[2] is None)
    fail = sum(1 for s in summary if s[2] is not None)
    print(f'  成功: {succ} 失败: {fail} 输出目录: {output_dir}')


if __name__ == '__main__':
    main()
