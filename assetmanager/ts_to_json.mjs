def ts_to_json(ts_path: Path, json_path: Path) -> None:
    """
    Convert TS -> JSON.
    1) Try to detect a pure object literal and parse it fast in Python.
    2) If the object looks computed OR the literal path fails for any reason,
       automatically fall back to the Node helper (ts_to_json.mjs).
    """
    raw = ts_path.read_text(encoding="utf-8")
    # Try to extract the exported object; if that already fails, go straight to Node.
    try:
        obj_src = _extract_export_default_object(raw)
    except Exception:
        obj_src = None  # force node fallback

    def _run_node():
        node = _node_bin()
        helper = Path(__file__).with_name("ts_to_json.mjs")
        if not node:
            raise RuntimeError(
                "Node.js is required for TS conversion but was not found. "
                "Install Node 18+ and ensure `node` is in PATH, or set NODE_BIN in config.py."
            )
        if not helper.exists():
            raise RuntimeError(
                "Missing ts_to_json.mjs next to etl.py. Create the helper and make it executable. "
                "It must be able to require 'ts-node' and 'typescript'."
            )
        res = subprocess.run(
            [node, str(helper), str(ts_path)],
            capture_output=True,
            text=True,
            env=_node_env_with_global_node_path(),
        )
        if res.returncode != 0:
            snippet = (res.stderr or res.stdout or "").strip()
            if len(snippet) > 800:
                snippet = snippet[:800] + " …"
            raise RuntimeError(f"TS->JSON conversion failed (node): {snippet}")
        json_path.write_text(res.stdout, encoding="utf-8")

    # If we couldn't extract or it obviously looks computed, just use Node.
    if obj_src is None or _looks_non_literal(obj_src):
        _run_node()
        return

    # Literal path first; if that blows up, we auto-fallback to Node.
    try:
        json_like = _ts_object_to_json_text(obj_src)
        # Some LSSM maps sneak in values that still aren't valid JSON;
        # if json.loads fails, we'll catch and fallback.
        obj = json.loads(json_like)
        json_path.write_text(json.dumps(obj), encoding="utf-8")
    except Exception:
        # Last resort: evaluate with Node.
        _run_node()
