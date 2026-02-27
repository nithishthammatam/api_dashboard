from database.firebase import db, init_error
from routers.dashboard import build_user_activity_index


def main():
    if not db:
        raise RuntimeError(f"Database connection not initialized. Error: {init_error}")

    stats = build_user_activity_index()
    print(
        "[user-activity-index] completed "
        f"indexed_users={stats.get('indexed_users', 0)}, "
        f"docs_scanned={stats.get('docs_scanned', 0)}, "
        f"screentime_docs={stats.get('screentime_docs', 0)}"
    )


if __name__ == "__main__":
    main()

