import subprocess
import sys

tests = [
    "contention_test.py",
    "duplicate_checkout_test.py",
    "timeout_test.py",
    "crash_test.py",
    "throughput_test.py"
]

passed = 0

for t in tests:
    print("\n========================")
    print("Running", t)
    print("========================")

    result = subprocess.run(
        ["python", f"{t}"],
        capture_output=True,
        text=True
    )

    print(result.stdout)
    print(result.stderr)

    if result.stdout == "" or result.stdout == "Resetting system state\n":
        print(f"Error running test {t}")
    elif "FAIL" in result.stdout:
        print(t, "FAILED")
    else:
        print(t, "PASSED")
        passed += 1

print("\n========================")
print(f"{passed}/{len(tests)} tests passed")
print("========================")

if passed != len(tests):
    sys.exit(1)