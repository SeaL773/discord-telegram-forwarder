# Safe Public Release Workflow

This document describes how to verify and clean Git history before publishing this repository. The current working tree is sanitized. Older commits contain numeric deployment identifiers (chat/channel/guild IDs) that are not credentials but do identify private communities.

Two paths are available. Pick one and complete it fully before publishing.

---

## Path A: Fresh public repository (recommended)

This is the safest option. It publishes only the reviewed current tree with no prior history.

1. **Back up the private repository first.**

   ```sh
   cp -a /path/to/discord-tg-forwarder /path/to/discord-tg-forwarder.bak
   ```

2. **Verify the current tree is clean** using the audit script (see below).

3. **Create a new empty repository** on GitHub (or your host). Do not initialize it with a README.

4. **Export the reviewed working tree into a separate directory.**
   Run this from a POSIX shell or Git Bash. It copies modified tracked files
   and non-ignored untracked files, including the reviewed release additions,
   without modifying the private repository. Ignored files such as `.env`,
   private `.local` files, runtime data, and build output are excluded.

   ```sh
   SOURCE=/path/to/discord-tg-forwarder
   EXPORT=/path/to/discord-tg-forwarder-public
   mkdir -p "$EXPORT"
   git -C "$SOURCE" ls-files -z --cached --others --exclude-standard |
     tar -C "$SOURCE" --null --no-recursion -T - -cf - |
     tar -xf - -C "$EXPORT"
   ```

   Inspect the export before initializing Git. Confirm that `LICENSE`, the
   release workflow, audit script, tests, and all intended source changes are
   present, and that `.git`, `.env`, private `.local` files, `/data`, logs, and
   build artifacts are absent.

5. **Initialize a fresh Git repository in the export directory and make the first commit.**

   ```sh
   cd /path/to/discord-tg-forwarder-public
   git init -b main
   git add -A
   git commit -m "chore: initial public release"
   ```

6. **Run the audit script against the new repository** to confirm no identifiers leaked.

   ```sh
   python /path/to/discord-tg-forwarder/scripts/audit_history.py \
     --repo /path/to/discord-tg-forwarder-public \
     --denylist /path/to/denylist.txt
   ```

7. **Run a secret scanner** (Gitleaks or TruffleHog) against the new repository as a final check.

8. **Add the remote and push manually** when satisfied.

   ```sh
   git remote add origin https://github.com/yourname/discord-tg-forwarder-public.git
   git push -u origin main
   ```

---

## Path B: Rewrite history in place (destructive, requires coordination)

Use this only if you need to preserve commit history. It rewrites all affected refs and requires a force-push. Anyone with a clone must re-clone afterward.

**Prerequisites:** Install `git-filter-repo` (`pip install git-filter-repo`).

1. **Back up the repository.**

   ```sh
   cp -a /path/to/discord-tg-forwarder /path/to/discord-tg-forwarder.bak
   ```

2. **Run the audit script** to identify which commits contain identifiers (see below). Record the object SHAs from the output.

3. **Create a replacements file** mapping each private identifier to a synthetic placeholder. Do not commit this file.

   ```
   REAL_GUILD_ID==>EXAMPLE_GUILD_ID
   REAL_CHANNEL_ID==>EXAMPLE_CHANNEL_ID
   REAL_CHAT_ID==>EXAMPLE_CHAT_ID
   ```

4. **Rewrite all refs.**

   ```sh
   cd /path/to/discord-tg-forwarder
   git filter-repo --replace-text /path/to/replacements.txt --force
   ```

5. **Verify the rewrite** by running the audit script again. It must exit 0.

6. **Run a secret scanner** (Gitleaks or TruffleHog) across all refs.

7. **Force-push all refs** to the remote. Notify all collaborators to re-clone.

   ```sh
   git push --force-with-lease --all
   git push --force-with-lease --tags
   ```

8. **Confirm CI artifacts, release archives, and any mirrors** do not contain the old history.

---

## Using the audit script

`scripts/audit_history.py` scans reachable blobs, commit messages, and annotated tag messages for exact strings you supply. It never prints matched values, only metadata (object SHA/type, paths within the tree, containing refs, term index, and match count).

**Prepare a denylist file** (UTF-8, one entry per line, max 200 chars each, max 500 entries, no real IDs in tracked files):

```
# denylist.txt -- keep this file private, do not commit
# Replace the lines below with your actual deployment IDs (guild, channel, chat IDs).
YOUR_DISCORD_GUILD_ID_HERE
YOUR_DISCORD_CHANNEL_ID_HERE
YOUR_TELEGRAM_CHAT_ID_HERE
```

**Run the audit:**

```sh
python scripts/audit_history.py \
  --repo /path/to/discord-tg-forwarder \
  --denylist /path/to/denylist.txt
```

Exit codes:
- `0` -- clean, no denylist entries found in any reachable object
- `1` -- findings present; output lists affected objects and refs (values redacted)
- `2` -- usage or validation error (bad denylist, not a git repo, etc.)

Run this after every history rewrite and before every push to a public remote. Also scan all branches and tags, not just `main`.

---

## After publishing

- Confirm `.env`, `.local/`, `/data`, catalog files, logs, backups, and Compose overrides are absent from the release and CI artifacts.
- Re-run the test, compile, and Compose validation commands from a clean checkout:

  ```sh
  docker run --rm -v "$PWD:/workspace:ro" python:3.12-slim sh -c \
    'mkdir /tmp/project && cp -a /workspace/. /tmp/project/ && cd /tmp/project && \
     pip install -r requirements-dev.txt && pytest -q && \
     python -m compileall -q src tests scripts'
  docker compose config
  ```

- Keep the private repository and its backup separate from the public one.
