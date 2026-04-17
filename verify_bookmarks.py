import sqlite3
conn = sqlite3.connect('data/bundeshaushalt.db')
total = conn.execute('SELECT COUNT(*) FROM pdf_bookmarks').fetchone()[0]
years = conn.execute('SELECT COUNT(DISTINCT year) FROM pdf_bookmarks').fetchone()[0]
print(f'Bookmarks: {total} across {years} years')
# Check EP06 2021 ueberblick
rows = conn.execute("SELECT title, page_number FROM pdf_bookmarks WHERE year=2021 AND nav_type='ep_ueberblick' AND einzelplan='06'").fetchall()
print(f'EP06 2021 overview: {rows}')
conn.close()
