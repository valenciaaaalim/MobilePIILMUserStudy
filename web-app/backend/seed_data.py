"""
Seed script - no longer used.

Conversations are no longer stored in the database. They are loaded directly
from backend/app/assets/annotated_test.json by the /api/conversations/seed endpoint at runtime.
The database schema now has only 7 normalized tables (participants,
baseline_assessment, scenario_responses, post_scenario_survey, pii_disclosure,
sus_responses, end_of_study_survey).

If you run this script, it will only initialize the database tables (no conversation seeding).
"""
import sys
from app.database import init_db


def main():
    """Initialize database tables only. No conversation seeding."""
    init_db()
    print("Database initialized. Conversations are served from backend/app/assets/annotated_test.json via /api/conversations/seed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
