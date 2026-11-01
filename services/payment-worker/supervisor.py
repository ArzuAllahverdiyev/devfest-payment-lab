import os
import subprocess
import sys
import threading
import time

import psycopg


def connection():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )


def log(conn, component, message):
    print(f"[{component}] {message}", flush=True)
    conn.execute(
        """
        INSERT INTO demo.worker_logs (component, message)
        VALUES (%s, %s)
        """,
        (component, message[:1000]),
    )


def forward_logs(process):
    try:
        with connection() as conn:
            for line in process.stdout:
                message = line.rstrip()
                print(message, flush=True)
                conn.execute(
                    """
                    INSERT INTO demo.worker_logs
                        (component, message)
                    VALUES ('WORKER', %s)
                    """,
                    (message[:1000],),
                )
    except Exception as exc:
        print(f"[SUPERVISOR] Log capture error: {exc}", flush=True)


def set_state(conn, state, pid=None):
    conn.execute(
        """
        UPDATE demo.worker_runtime
        SET actual_state = %s,
            process_pid = %s,
            updated_at = NOW()
        WHERE id = 1
        """,
        (state, pid),
    )


def main():
    process = None

    with connection() as conn:
        log(conn, "SUPERVISOR", "Payment Worker supervisor started")

        while True:
            desired = conn.execute(
                """
                SELECT desired_state
                FROM demo.worker_runtime
                WHERE id = 1
                """
            ).fetchone()[0]

            if process is not None and process.poll() is not None:
                code = process.returncode
                log(conn, "SUPERVISOR", f"Worker exited: {code}")
                process = None

            if desired == "STOPPED":
                if process is not None:
                    process.kill()
                    process.wait()
                    process = None

                    log(
                        conn, "SUPERVISOR",
                        "Consumer killed with SIGKILL; "
                        "offset may remain uncommitted",
                    )
                    set_state(conn, "CRASHED")

                # Preserve CRASHED until presenter requests recovery.
                time.sleep(0.5)
                continue

            if process is None:
                process = subprocess.Popen(
                    [sys.executable, "-u", "/app/main.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                threading.Thread(
                    target=forward_logs,
                    args=(process,),
                    daemon=True,
                ).start()

                set_state(conn, "RUNNING", process.pid)
                log(
                    conn, "SUPERVISOR",
                    f"Consumer started: PID={process.pid}",
                )

            time.sleep(0.5)


if __name__ == "__main__":
    main()
