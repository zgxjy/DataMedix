# 打包 DataMedix 应用指南

本文档介绍了使用 PyInstaller 将 应用打包成 Windows 独立可执行文件 (.exe) 的步骤。遵循这些步骤可以确保一个干净的构建环境，避免与系统已安装的其他 Python 环境（如 Anaconda）产生冲突。

## 先决条件

-   已安装 Python (推荐版本 3.9 或更高版本)。
-   项目的源代码已准备就绪。
-   网络连接（用于下载依赖包）。

## 打包步骤

1.  **创建并激活虚拟环境:**
    打开命令提示符 (CMD) 或 PowerShell，导航到项目根目录 ，然后执行以下命令：

    ```bash
    # 创建虚拟环境（如果 .venv 文件夹不存在）
    python -m venv .venv
    
    # 激活虚拟环境
    # 在 PowerShell 中:
    .\.venv\Scripts\Activate.ps1
    # 或者在 CMD 中:
    .\.venv\Scripts\activate.bat
    ```

    成功激活后，你的命令行提示符前会显示 `(.venv)`。**后续所有命令都应在此激活的环境中执行。**

2.  **安装/更新 `pip` 和 `wheel`:**
    为了确保使用最新的包管理工具，运行：

    ```bash
    python -m pip install --upgrade pip wheel -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```
    *(注：这里使用了清华大学的 PyPI 镜像源以加速下载，你可以根据需要替换或移除 `-i` 及之后的部分以使用默认源)*

3.  **安装项目依赖:**
    使用 `requirements.txt` 文件安装项目所需的所有库：

    ```bash
    python -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```
    *(确保 `requirements.txt` 包含 `PySide6`, `psycopg2-binary`, `pandas` 等所有必要的包)*

4.  **安装 PyInstaller:**
    在虚拟环境中安装 PyInstaller：

    ```bash
    python -m pip install pyinstaller -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```

5.  **清理旧的构建文件 (可选但推荐):**
    如果在之前尝试过打包，最好删除旧的构建产物以避免潜在问题：

    ```powershell
    # 在 PowerShell 中:
    Remove-Item -Recurse -Force build, dist, *.spec
    # 或者在 CMD 中手动删除 build 文件夹、dist 文件夹和 .spec 文件
    ```

6.  **运行 PyInstaller 打包命令:**
    执行以下命令开始打包过程：

    ```bash
    pyinstaller --name MedicalDataExtractor --onefile --windowed medical_data_extractor.py
    ```

    ```bash
    pyinstaller --name MedicalDataExtractor --onefile --windowed --icon=assets/icons/icon.ico --hidden-import=scipy._cyutility medical_data_extractor.py
    ```
    -   `--icon=your_icon.ico`:  (可选) 指定可执行文件的图标（ICO 文件）。
    -   `--name MedicalDataExtractor`: 指定生成的可执行文件名和相关文件夹名。
    -   `--onefile`: 将所有内容打包到一个单独的 `.exe` 文件中。
    -   `--windowed`: 创建一个窗口应用程序（隐藏控制台窗口）。
    -   `medical_data_extractor.py`: 指定你的应用主入口脚本。

7.  **获取可执行文件:**
    如果打包成功，你将在项目根目录下的 `dist` 文件夹中找到最终的可执行文件 `MedicalDataExtractor.exe`。你可以将这个文件分发给其他人使用。

## 注意事项

-   务必在**激活的虚拟环境**中执行所有 `pip install` 和 `pyinstaller` 命令。
-   如果遇到 `pathlib` 或其他与 Anaconda 相关的库冲突错误，请严格检查是否确实在干净的虚拟环境中操作，并考虑清理 PyInstaller 缓存 (`%APPDATA%\pyinstaller` 或 `%LOCALAPPDATA%\pyinstaller`)。
-   使用国内镜像源 (如清华源、阿里源等) 可以显著提高 `pip` 下载速度。