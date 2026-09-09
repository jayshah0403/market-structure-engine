# Working rules for this repo

- Read docs/CURRENT_STATE.md and docs/V2_SPEC.md fully before any change.
- One PR per spec section. Implement only that section. If something outside it
  looks wrong, note it in the PR description — do not fix it.
- Write the section's acceptance tests first; they must fail on current code and
  pass after. Run `python -m pytest -v` and paste the output in the PR.
- Engine code never imports FastAPI. HTTP concerns live only in the API layer.
- No hardcoded symbol, bucket size, period, or URL in engine code — read from instruments.
- Never touch, print, or commit .env or any credential.
- If the spec is ambiguous, stop and ask; do not guess. List every ambiguity you hit.
- End every PR with a per-file explanation of what changed and why, written so a
  reviewer who did not write the code can follow each hunk.
