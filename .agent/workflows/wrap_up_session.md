---
description: How to wrap up a coding session for the Drehem Tablet Analysis project
---

# Session Wrap-Up Protocol

Whenever concluding a significant coding session or when the user asks to wrap up, follow this workflow to ensure work is logged and committed properly. This ensures continuity across context wipes.

// turbo-all
1. Verify what changes were made during the session using `git status` and `git diff`.
2. Write a detailed session log in the `logs/` directory using markdown format (e.g., `logs/YYYY-MM-DD_session_summary.md`), detailing what was fixed, added, or changed, and any remaining open issues.
3. Stage all changed files, including the new log file, using `git add -A`.
4. Create a comprehensive commit message describing the work done in the session using `git commit -m "..."`.
5. Push the changes to the remote repository using `git push`.
6. Notify the user with a bulleted summary of the changes and confirm that the work has been logged, committed, and pushed.
