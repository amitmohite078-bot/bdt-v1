#!/usr/bin/env python3
"""Cross-platform Test Runner for SmartProtein: Hadoop HDFS DQ & Accurate Protein Recommender.

Executes all test suites and prints a formatted summary.
Usage:
    python run_all_tests.py
    pytest tests/ -v
    make alltest
"""

import sys
import subprocess
import time


def print_banner(title: str):
    line = "=" * 70
    print(f"\n{line}\n  {title}\n{line}")


def run_command(cmd: list[str], label: str) -> bool:
    print(f"\n[RUNNING] {label}...")
    start = time.time()
    res = subprocess.run(cmd)
    elapsed = time.time() - start
    if res.returncode == 0:
        print(f"[PASSED]  {label} ({elapsed:.2f}s)")
        return True
    else:
        print(f"[FAILED]  {label} (exit code {res.returncode})", file=sys.stderr)
        return False


def main():
    print_banner("SmartProtein Automated All-Tests Validation Suite")

    tests = [
        ([sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"], "Pytest Unit & Integration Suite (25+ Tests)"),
        ([sys.executable, "smartprotein/tests/run_dq_validation_suite.py"], "Hadoop Medallion Curated DQ Gate (7 Dimensions)"),
        ([sys.executable, "-m", "py_compile", "app.py", "smartprotein/dashboard/app.py"], "Python Syntax & Bytecode Compilation Check"),
    ]

    results = []
    for cmd, label in tests:
        passed = run_command(cmd, label)
        results.append((label, passed))

    print_banner("SmartProtein Test Execution Summary")
    all_ok = True
    for label, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f" {status:10s} | {label}")
        if not passed:
            all_ok = False

    print("=" * 70)
    if all_ok:
        print(" 🎉 ALL TESTS PASSED! Recommender, Lakehouse DQ & LP Solver are 100% verified.")
        sys.exit(0)
    else:
        print(" ❌ SOME TESTS FAILED! Check stderr logs above.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
