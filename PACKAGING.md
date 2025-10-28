# 打包 DataMedix 应用指南

本文档介绍了使用 PyInstaller 将 **DataMedix (MedicalDataExtractor)** 应用打包成独立可执行文件的步骤。

## 重要前提：关于跨平台打包

PyInstaller **不支持交叉编译**。这意味着：

*   **你必须在 Windows 系统上** 才能打包成 Windows 的 `.exe` 文件。
*   **你必须在 macOS 系统上** 才能打包成 macOS 的 `.app` 程序包。

你无法在Windows电脑上直接为Mac用户生成可执行文件。以下指南分为两个部分，请在对应的操作系统上执行。

---

## Windows 打包指南

本部分介绍了在 Windows 上打包成 `.exe` 文件的步骤。

### 先决条件

*   一台运行 Windows 的电脑。
*   已安装 Python (推荐版本 3.9 或更高版本)。
*   项目的源代码。

### 打包步骤

1.  **创建并激活虚拟环境:**
    打开命令提示符 (CMD) 或 PowerShell，导航到项目根目录，然后执行以下命令：

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
    ```    *(注：这里使用了清华大学的 PyPI 镜像源以加速下载)*

3.  **安装项目依赖:**
    使用 `requirements.txt` 文件安装项目所需的所有库：

    ```bash
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```

4.  **安装 PyInstaller:**
    在虚拟环境中安装 PyInstaller：

    ```bash
    pip install pyinstaller -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```

5.  **清理旧的构建文件 (可选但推荐):**
    如果在之前尝试过打包，最好删除旧的构建产物以避免潜在问题：

    ```powershell
    # 在 PowerShell 中:
    Remove-Item -Recurse -Force build, dist, *.spec
    # 或者在 CMD 中手动删除 build 文件夹、dist 文件夹和 .spec 文件
    ```

6.  **运行 PyInstaller 打包命令:**
    执行以下命令开始打包过程。`.ico` 文件是Windows的图标格式。

    ```bash
    pyinstaller --name "MedicalDataExtractor" --onefile --windowed --icon=assets/icons/icon.ico --hidden-import=scipy._cyutility medical_data_extractor.py
    ```
    *   `--name "MedicalDataExtractor"`: 指定生成的可执行文件名。
    *   `--onefile`: 将所有内容打包到一个单独的 `.exe` 文件中。
    *   `--windowed`: 创建一个窗口应用程序（隐藏命令行控制台）。
    *   `--icon=assets/icons/icon.ico`: 指定应用的图标文件。
    *   `--hidden-import=scipy._cyutility`: 显式包含一些PyInstaller可能找不到的隐藏依赖。
    *   `medical_data_extractor.py`: 指定你的应用主入口脚本。

7.  **获取可执行文件:**
    打包成功后，你将在项目根目录下的 `dist` 文件夹中找到最终的可执行文件 `MedicalDataExtractor.exe`。

---

## macOS 打包指南

本部分介绍了在 macOS 上打包成 `.app` 应用程序的步骤。你**必须**在一台Mac上执行这些操作。

### 先决条件

*   一台运行 macOS 的电脑。
*   已安装 Python 3 (推荐通过 Homebrew 或官方安装器安装)。
*   项目的源代码。
*   一个 `.icns` 格式的图标文件 (Windows 的 `.ico` 文件在Mac上无效)。你可以使用在线转换工具将 `.png` 或 `.ico` 转换为 `.icns`。

### 打包步骤

1.  **创建并激活虚拟环境:**
    打开 **终端 (Terminal)** 应用，导航到项目根目录，然后执行以下命令：

    ```bash
    # 创建虚拟环境（如果 .venv 文件夹不存在）
    python3 -m venv .venv
    
    # 激活虚拟环境
    source .venv/bin/activate
    ```

    成功激活后，你的命令行提示符前会显示 `(.venv)`。**后续所有命令都应在此激活的环境中执行。**

2.  **安装/更新 `pip` 和 `wheel`:**
    
    ```bash
    python3 -m pip install --upgrade pip wheel -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```

3.  **安装项目依赖:**
    
    ```bash
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```

4.  **安装 PyInstaller:**
    
    ```bash
    pip install pyinstaller -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
    ```

5.  **清理旧的构建文件 (可选但推荐):**
    
    ```bash
    rm -rf build/ dist/ *.spec
    ```

6.  **运行 PyInstaller 打包命令:**
    执行以下命令开始打包。在macOS上，`--windowed` 选项会自动创建一个 `.app` 应用程序包。

    ```bash
    pyinstaller --name "MedicalDataExtractor" --onefile --windowed --icon=assets/icons/icon.icns --hidden-import=scipy._cyutility medical_data_extractor.py
    ```
    *   `--icon=assets/icons/icon.icns`: **注意**，这里必须使用 `.icns` 格式的图标文件。

7.  **获取应用程序:**
    打包成功后，你将在 `dist` 文件夹中找到 `MedicalDataExtractor.app`。这是一个标准的macOS应用程序，可以双击运行。

### 进阶：创建 .dmg 磁盘映像（推荐）

为了方便分发，最好将 `.app` 文件打包成一个 `.dmg` 磁盘映像文件。

```bash
# 确保你在项目的根目录下
# 创建一个临时文件夹来存放 .app 文件
mkdir -p dmg_build/

# 将打包好的 .app 复制到临时文件夹
cp -R dist/MedicalDataExtractor.app dmg_build/

# 使用 hdiutil 命令创建 .dmg 文件
hdiutil create -volname "MedicalDataExtractor" -srcfolder dmg_build -ov -format UDZO dist/MedicalDataExtractor.dmg

# 清理临时文件夹
rm -rf dmg_build/
```

之后你就可以将 `dist/MedicalDataExtractor.dmg` 文件分发给其他Mac用户了。