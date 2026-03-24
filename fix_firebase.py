content = open('database/firebase.py', encoding='utf-8').read()

old_block = """        # Handle escaped newlines (common in .env files)
        if "\\\\n" in private_key:
             print("   Refining private key (replacing \\\\n with newlines)")
             private_key = private_key.replace("\\\\n", "\\n")
        
        # Ensure correct header/footer
        if "-----BEGIN PRIVATE KEY-----" not in private_key:
             print("[firebase] WARNING: Private key missing header")"""

new_block = """        # --- Robust private key normalization for Render/Railway ---
        # Strip surrounding quotes Render may add
        private_key = private_key.strip().strip('"').strip("'")

        # Case 1: Escaped \\n in env var (most common on Render)
        if "\\\\n" in private_key:
            print("   Refining private key (replacing \\\\n with newlines)")
            private_key = private_key.replace("\\\\n", "\\n")

        # Case 2: All on one line - reconstruct PEM newlines
        if "-----BEGIN PRIVATE KEY-----" in private_key and "\\n" not in private_key:
            print("   Refining private key (reconstructing PEM newlines)")
            private_key = (
                private_key
                .replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\\n")
                .replace("-----END PRIVATE KEY-----", "\\n-----END PRIVATE KEY-----")
            )

        if "-----BEGIN PRIVATE KEY-----" not in private_key:
            print("[firebase] WARNING: Private key missing header")"""

if old_block in content:
    content = content.replace(old_block, new_block)
    open('database/firebase.py', 'w', encoding='utf-8').write(content)
    print("SUCCESS: firebase.py updated!")
else:
    # Try with \r\n line endings
    old_block_crlf = old_block.replace('\n', '\r\n')
    if old_block_crlf in content:
        content = content.replace(old_block_crlf, new_block)
        open('database/firebase.py', 'w', encoding='utf-8').write(content)
        print("SUCCESS: firebase.py updated (CRLF)!")
    else:
        print("Pattern not found. Printing lines 44-55:")
        for i, l in enumerate(content.splitlines()[43:55], 44):
            print(f"{i}: {repr(l)}")
