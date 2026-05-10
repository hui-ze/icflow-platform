"""
IC-Flow Platform CLI 入口

使用方式：
    icflow start      启动 API 服务
    icflow run-demo   运行端到端演示
    icflow status     检查项目状态
    icflow version    显示版本号

安装后通过 `icflow` 命令访问，或直接 `python -m icflow.cli`
"""

from __future__ import annotations

import argparse
import os
import sys
import subprocess


def _get_project_root() -> str:
    """获取项目根目录
    
    开发模式: src/icflow/cli.py -> src/ -> icflow 项目根目录
    安装模式: site-packages/icflow/cli.py 无法定位，回退到 CWD
    """
    path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # 如果路径以 src 结尾，再往上翻一级（开发模式）
    if os.path.basename(path) == "src":
        path = os.path.dirname(path)
    return path


def _get_version() -> str:
    """读取版本号"""
    try:
        from src.icflow import __version__
        return __version__
    except ImportError:
        return "0.1.0"


def cmd_version(args: argparse.Namespace) -> None:
    """显示版本号"""
    print(f"IC-Flow Platform v{_get_version()}")


def cmd_start(args: argparse.Namespace) -> None:
    """启动 API 服务"""
    host = args.host
    port = args.port
    reload_flag = "--reload" if args.reload else ""
    
    cmd = (
        f"uvicorn src.icflow.api.main:app "
        f"--host {host} --port {port} {reload_flag}"
    )
    
    print(f"启动 IC-Flow Platform API 服务...")
    print(f"  http://{host}:{port}")
    print(f"  Docs: http://{host}:{port}/docs")
    print()
    
    os.chdir(_get_project_root())
    os.system(cmd)


def cmd_run_demo(args: argparse.Namespace) -> None:
    """运行端到端演示"""
    root = _get_project_root()
    os.chdir(root)
    
    print("运行端到端演示...")
    print()
    
    result = subprocess.run(
        [sys.executable, "demo_end_to_end.py"],
        cwd=root,
    )
    sys.exit(result.returncode)


def cmd_status(args: argparse.Namespace) -> None:
    """检查项目状态"""
    root = _get_project_root()
    src_path = os.path.join(root, "src")
    env = os.environ.copy()
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")
    
    print(f"IC-Flow Platform 项目状态")
    print(f"{'=' * 40}")
    
    # 版本
    print(f"  Version: {_get_version()}")
    
    # 检查引擎文件完整性
    engine_dir = os.path.join(root, "src", "icflow", "engines")
    expected_engines = [
        "drc_repair.py",
        "lvs_repair.py",
        "eda_tool_adapter.py",
        "knowledge_management.py",
        "workflow_orchestrator.py",
    ]
    
    print(f"\n  引擎组件:")
    for engine_file in expected_engines:
        path = os.path.join(engine_dir, engine_file)
        exists = os.path.exists(path)
        status = "✅" if exists else "❌"
        print(f"    {status} {engine_file}")
    
    # API 服务
    api_dir = os.path.join(root, "src", "icflow", "api")
    api_files = ["main.py", "routes.py", "schemas.py", "auth.py"]
    print(f"\n  API 服务:")
    for f in api_files:
        exists = os.path.exists(os.path.join(api_dir, f))
        status = "✅" if exists else "❌"
        print(f"    {status} {f}")
    
    # 部署文件
    deploy_exists = os.path.exists(os.path.join(root, "Dockerfile"))
    compose_exists = os.path.exists(os.path.join(root, "docker-compose.yml"))
    k8s_exists = os.path.exists(os.path.join(root, "deploy", "k8s"))
    print(f"\n  部署方案:")
    print(f"    {'✅' if deploy_exists else '❌'} Dockerfile")
    print(f"    {'✅' if compose_exists else '❌'} docker-compose.yml")
    print(f"    {'✅' if k8s_exists else '❌'} K8s 清单")
    
    # 测试状态
    print(f"\n  测试状态:")
    test_result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "--tb=no", "-q"],
        cwd=root,
        capture_output=True,
        text=True,
        env=env,
    )
    
    if test_result.returncode == 0:
        # 提取摘要行
        lines = test_result.stdout.strip().split("\n")
        summary = lines[-1] if lines else "全部通过"
        if "passed" in summary:
            print(f"    ✅ {summary}")
        else:
            print(f"    {summary}")
    else:
        print(f"    ❌ 测试失败 (exit code: {test_result.returncode})")
        print(f"       {test_result.stdout.strip().split(chr(10))[-1]}")


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        description="IC-Flow Platform - 芯片设计流程自动化平台",
    )
    parser.add_argument(
        "--version", action="store_true",
        help="显示版本号后退出"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # start 命令
    start_parser = subparsers.add_parser("start", help="启动 API 服务")
    start_parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    start_parser.add_argument("--port", type=int, default=8000, help="监听端口")
    start_parser.add_argument("--reload", action="store_true", help="热重载模式")
    
    # run-demo 命令
    subparsers.add_parser("run-demo", help="运行端到端演示")
    
    # status 命令
    subparsers.add_parser("status", help="检查项目状态")
    
    # 解析参数
    args = parser.parse_args()
    
    if args.version:
        cmd_version(args)
        return
    
    if args.command == "start":
        cmd_start(args)
    elif args.command == "run-demo":
        cmd_run_demo(args)
    elif args.command == "status":
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
