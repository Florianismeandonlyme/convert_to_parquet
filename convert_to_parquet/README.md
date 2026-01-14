# convert_to_parquet

批量将目录下的 `csv` / `xlsx` / `dta` 文件转换为 Parquet，并在输出目录中镜像原始目录结构（可直接作为 Hive 表目录）。

快速开始

1. 创建虚拟环境并安装依赖：

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2. 运行脚本：

```bash
python convert_to_parquet.py --input "D:\\path\\to\\your\\folder" --output "D:\\path\\to\\out_folder"
```

若不传 `--input`，脚本会弹出选择文件夹对话框（Windows）。

默认输出目录为输入目录下的 `_parquet_out`。

删除原文件

脚本会在完成转换后询问是否删除原始文件；如果你希望自动删除，可加 `--delete` 参数（谨慎使用）。

打包为 exe（可选）

安装 `pyinstaller` 后：

```bash
pip install pyinstaller
pyinstaller -F convert_to_parquet.py
```

这会在 `dist` 生成单文件 exe（Windows）。

注意与限制

- 对大 CSV（>50MB）采用 chunk 分块输出为多个 part-xxxxx.parquet 文件。
- Excel 仅读取第一个 sheet（如需全部 sheet，可修改脚本）。
- 确保有足够磁盘空间和权限写入输出目录。
