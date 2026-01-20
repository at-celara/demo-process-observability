## Demo: Process Observability — Stage 0

A minimal demo repository that establishes a stable run contract and CLI scaffold. Stage 0 includes no data parsing and no LLM calls.

### What this provides

- `uv run demo run` loads the dataset, normalizes messages, writes JSONL, and updates `run_meta.json`
- `uv run demo eval` exists as a stub and prints helpful output
- Config and env conventions are in place

### Requirements

- Python >= 3.11

### Setup

You can use `uv`.

Install `uv` if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# ensure ~/.local/bin is on PATH for the current shell session
export PATH="$HOME/.local/bin:$PATH"
```

Install dependencies and the project (editable):

```bash
uv sync
```

Quickstart sanity check:

```bash
# Run console script via uv
uv run demo --help

# Or module form
uv run -m demo.cli --help
```

Note: Run commands from the repository root.

### Stage 1 usage

Show CLI help:

```bash
uv run demo --help
```

Create a run (loads dataset, normalizes messages, writes outputs):

```bash
uv run demo run --input data/01_raw_messages.json
```

This creates:

- `runs/<run_id>/messages.normalized.jsonl`
- `runs/<run_id>/run_meta.json` (with `stage: 1`, counts, and stats)

Run the eval stub:

```bash
uv run demo eval --run-id <run_id>
```

### Config

Default `config.yml`:

```yaml
project:
  name: demo-process-observability
io:
  runs_dir: runs
  input_path: data/01_raw_messages.json
  output:
    normalized_messages: messages.normalized.jsonl
normalize:
  sort_by_timestamp: true
  keep_raw: true
  allow_empty_text: true
run:
  write_run_meta: true
eval:
  review_filename: review.json
  report_filename: eval_report.json
```

### Definition of Done (Stage 1)

```bash
uv sync
uv run demo run --input data/01_raw_messages.json
```

Confirm:
- `runs/<run_id>/messages.normalized.jsonl` exists and has the same number of lines as raw messages
- `runs/<run_id>/run_meta.json` has `stage: 1`, counts, `stats.by_source`, and `stats.top_threads`

### Roadmap

- Stage 3.5 will implement review/eval
