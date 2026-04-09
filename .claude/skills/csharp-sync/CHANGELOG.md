# Changelog Format

Write changelog entries in the style of [ravendb-python-client releases](https://github.com/ravendb/ravendb-python-client/releases).

## Structure

1. **Short intro paragraph** — one sentence per headline feature.
2. **Docs links** — link to relevant RavenDB documentation pages.
3. **PyPi link** — `https://pypi.org/project/ravendb/<version>/`
4. **`## Highlights`** — one subsection per major feature:
   - Brief prose description (2–3 sentences max).
   - One focused code sample (≤ 25 lines).
   - List of new classes / operations / exceptions.
5. **`### Other Changes`** — bullet list for smaller additions (new fields, wiring fixes, etc.).

## Code sample rules

- **Imports first**, then config, then usage — a reader should be able to copy-paste and run.
- Show the **happy path** and one **error/exception catch** if the feature raises a specific exception.
- Keep each sample **under 25 lines** of code.
- Use `snake_case` for Python, but `PascalCase` for JSON keys in `to_json()`/`from_json()`.

## Example

```markdown
### Feature Name

Brief description of the feature in 2–3 sentences.

\`\`\`python
from ravendb.documents.operations.feature import FeatureOperation, FeatureConfig

config = FeatureConfig(name="example")
store.maintenance.send(FeatureOperation(config))
\`\`\`

New operations: `FeatureOperation`, `GetFeatureOperation`
New classes: `FeatureConfig`, `FeatureSettings`
```

