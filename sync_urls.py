import os
import psycopg2

DATABASE_URL = os.getenv("DATABASE_URL")

FRANCHISES = [
    {
        "name": "Pokemon",
        "direct_files": [
            "Pokemon/Poke-30A.txt",
            "Pokemon/Poke-AH.txt",
            "Pokemon/Poke-DR.txt",
            "Pokemon/Poke-ME.txt",
            "Pokemon/Poke-PO.txt",
            "Pokemon/Poke-Other.txt"
        ]
    },
    {
        "name": "One Piece",
        "direct_files": [
            "One Piece/EB-02.txt",
            "One Piece/EB-03.txt",
            "One Piece/IB-V5.txt",
            "One Piece/IB-V6.txt",
            "One Piece/OP-13.txt",
            "One Piece/OP-14.txt",
            "One Piece/OP-Other.txt"
        ]
    }
]

def sync():
    if not DATABASE_URL:
        print("DATABASE_URL not set")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS monitored_urls (
            url TEXT NOT NULL,
            file_group TEXT NOT NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (url, file_group)
        )
    """)
    conn.commit()

    total_added = 0
    total_removed = 0

    for franchise in FRANCHISES:
        for file_path in franchise["direct_files"]:
            file_urls = set()
            try:
                with open(file_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and line.startswith("http"):
                            file_urls.add(line.split()[0])
            except FileNotFoundError:
                print(f"  File not found: {file_path}")
                continue

            cur.execute("SELECT url FROM monitored_urls WHERE file_group = %s", (file_path,))
            db_urls = set(row[0] for row in cur.fetchall())

            new_urls = file_urls - db_urls
            removed_urls = db_urls - file_urls

            for url in new_urls:
                cur.execute("INSERT INTO monitored_urls (url, file_group) VALUES (%s, %s) ON CONFLICT DO NOTHING", (url, file_path))
                total_added += 1
                print(f"  + {url} -> {file_path}")

            for url in removed_urls:
                cur.execute("DELETE FROM monitored_urls WHERE url = %s AND file_group = %s", (url, file_path))
                total_removed += 1
                print(f"  - {url} <- {file_path}")

            print(f"  {file_path}: {len(file_urls)} URLs ({len(new_urls)} new, {len(removed_urls)} removed)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone! {total_added} added, {total_removed} removed")
    print("The running bot will pick up these changes on the next scan cycle.")

if __name__ == "__main__":
    print("Syncing URL files to database...\n")
    sync()
