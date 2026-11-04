# Rules

1. Paths may point inside or outside the workspace. An outside path should be absolute.
2. Call next_context at the start of every step. Each call is one step; stepsLeft counts the steps the run has left.
3. Verify before you finish: run what you built and read its output.
4. finish needs a witness: one string that quotes the evidence you observed. Calls sent with finish may be refused.
5. Events older than the window are not in next_context. Query them with query_log. Do not guess.
6. Never print, store or request secrets, environment variables or API keys.
7. If the task cannot be done, finish with a witness that states why, with the evidence.
