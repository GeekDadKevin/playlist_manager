# Agent Policy: Catalog Refresh

- The agent must enforce that "Refresh Library Catalog" only updates the audio file database, never XML sidecars.
- If the refresh tool or code attempts to scan or gate on XML, the agent must propose and implement a fix to remove that logic.
- All XML handling is strictly delegated to other tools.
