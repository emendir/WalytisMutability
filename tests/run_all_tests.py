"""Run all tests in all variations."""

from emtest import ensure_dir_exists
from datetime import datetime
import os
import sys

from emtest import set_env_var, env_vars

TEST_FUNC_TIMEOUT_SEC = 300

WORKDIR = os.path.dirname(__file__)
os.chdir(WORKDIR)

pytest_args = sys.argv[1:]
# minimum python version for using extra pytest features
ADV_PYTEST_PY_VER = (3, 12)


WALY_TEST_REPORTS_DIR = env_vars.str(
    "WALY_TEST_REPORTS_DIR", default=os.path.join(WORKDIR, "reports")
)
REPORTS_DIR_PREF = os.path.join(WALY_TEST_REPORTS_DIR, "report-")


def run_tests() -> None:
    """Run each test file with pytest."""
    pytest_args = sys.argv[1:]
    timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")

    test_files = [
        (file.strip(".py"), os.path.join(WORKDIR, file))
        for file in os.listdir(WORKDIR)
        if (file.startswith("test_") and file.endswith(".py"))
    ]
    test_files.sort()
    for test_name, test_file in test_files:
        html_path = os.path.join(
            REPORTS_DIR_PREF + timestamp, test_name, "report.html"
        )
        json_path = os.path.join(
            REPORTS_DIR_PREF + timestamp, test_name, "report.json"
        )
        ensure_dir_exists(os.path.dirname(html_path))
        ensure_dir_exists(os.path.dirname(json_path))

        command = (
            f"{sys.executable} -m pytest {test_file} {' '.join(pytest_args)} "
        )
        if sys.version_info.minor >= ADV_PYTEST_PY_VER[1]:
            command += (
                f"--html={html_path} "
                f"--json={json_path} "
                f"--timeout={TEST_FUNC_TIMEOUT_SEC} "
            )
        os.system(command)


os.system("sudo systemctl start ipfs brenthy")

if True:
    os.chdir(WORKDIR)
    import conftest  # noqa
    # from build_docker import build_docker_image


# Test Procedure (Post-Prep)

set_env_var("WALYTIS_TEST_MODE", "RUN_BRENTHY")
print("Running tests with Brenthy...")
run_tests()

set_env_var("WALYTIS_TEST_MODE", "EMBEDDED")
print("Running tests with Walytis Embedded...")
run_tests()
os._exit(0)
