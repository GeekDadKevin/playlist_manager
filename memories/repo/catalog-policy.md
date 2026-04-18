# Catalog Refresh Policy

- The "Refresh Library Catalog" tool must only update the database to reflect the current state of audio files in the music root.
- It must remove database entries for files that no longer exist and add new entries for any new files found.
- It must NOT scan, index, or gate on XML sidecars or their state.
- All XML validation, repair, or reporting must be handled by separate tools, never as part of the catalog refresh.
- This is a hard requirement for all future agent and code changes.
