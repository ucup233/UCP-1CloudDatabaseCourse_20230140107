import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, InsertOne
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

# MongoDB connection
client = MongoClient(
    'mongodb+srv://praktikum:RvMwaD0nR1zMMx98@praktikum.cm6fe7p.mongodb.net/Praktikum?appName=Praktikum&tlsAllowInvalidCertificates=true'
)

collections = client['Praktikum']['UCP1']

# Check MongoDB connection
try:
    client.admin.command('ping')
    print('✅ Connected to MongoDB Atlas successfully!')
except Exception as e:
    print(f'❌ MongoDB connection failed: {e}')

session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0"
}

# Counter untuk membatasi total dokumen
total_documents = 0
MAX_DOCUMENTS = 10

def get_article_detail(link):
    try:
        res = session.get(link, headers=headers, timeout=10)
        soup = BeautifulSoup(res.text, 'html.parser') # Gunakan html.parser jika tidak ada lxml

        # Menyesuaikan meta property untuk CNBC Indonesia
        tanggal = soup.find('meta', attrs={'name': 'dtk:publishdate'}) or soup.find('meta', attrs={'name': 'publishdate'})
        author = soup.find('meta', attrs={'name': 'dtk:author'}) or soup.find('meta', attrs={'name': 'author'})
        tags = soup.find('meta', attrs={'name': 'dtk:keywords'}) or soup.find('meta', attrs={'name': 'keywords'})

        tanggal = tanggal['content'] if tanggal else None
        author = author['content'] if author else None
        tags = tags['content'] if tags else None

        # Class body berita CNBC biasanya 'detail-text'
        body = soup.find('div', class_='detail-text')
        isi_berita = ''

        if body:
            isi_berita = ' '.join(
                [p.get_text(strip=True) for p in body.find_all('p')]
            )

        thumbnail = soup.find('meta', attrs={'property': 'og:image'})
        thumbnail = thumbnail['content'] if thumbnail else None

        return {
            'tanggal_publish': tanggal,
            'author': author,
            'tag_kategori': tags,
            'isi_berita': isi_berita,
            'thumbnail': thumbnail,
        }

    except Exception as e:
        print(f'❌ Error detail: {e}')
        return None


def crawl_cnbc_keyword():
    global total_documents
    # Endpoint pencarian CNBC untuk Environmental Sustainability
    base_url = 'https://www.cnbcindonesia.com/search?query=environmental+sustainability'
    max_page = 2

    # Menulis log kapan script dijalankan (Berguna untuk melihat apakah Cronjob bekerja)
    print(f"\n==============================================")
    print(f"🕒 Memulai crawling pada: {datetime.datetime.now()}")
    print(f"==============================================\n")

    for page in range(1, max_page + 1):
        if total_documents >= MAX_DOCUMENTS:
            break
            
        url = f'{base_url}&page={page}'
        print(f'📄 Halaman {page}')

        try:
            res = session.get(url, headers=headers)
            soup = BeautifulSoup(res.text, 'html.parser')

            # Deteksi artikel
            articles = soup.find_all('article')
            if not articles:
                # Jika halaman search dirender menggunakan JS oleh CNBC,
                # Kita akan fallback mencoba membaca halaman indeks terbaru jika tidak ditemukan artikel
                if page == 1:
                    print("⚠️ Artikel tidak ditemukan menggunakan format search, mencoba membaca dari halaman indeks terbaru...")
                    res = session.get("https://www.cnbcindonesia.com/indeks", headers=headers)
                    soup = BeautifulSoup(res.text, 'html.parser')
                    articles = soup.find_all('article')
                    
                if not articles:
                    print('⚠️  Tidak ada artikel untuk di-scrape di halaman ini, berhentikan pencarian.')
                    break

            tasks = []
            results = []

            # Threading disini
            with ThreadPoolExecutor(max_workers=5) as executor:
                for artikel in articles:
                    if total_documents >= MAX_DOCUMENTS:
                        break
                        
                    link_tag = artikel.find('a')
                    # Judul utama di cnbc rata-rata ada pada h2
                    judul_tag = artikel.find('h2')

                    if not link_tag or not judul_tag:
                        continue

                    link = link_tag.get('href', '')
                    judul = judul_tag.text.strip()
                    
                    if not link.startswith('http'):
                        continue

                    # submit ke thread
                    future = executor.submit(get_article_detail, link)
                    tasks.append((future, judul, link))

                for future, judul, link in tasks:
                    if total_documents >= MAX_DOCUMENTS:
                        break
                        
                    detail = future.result()
                    if detail:
                        data = {
                            'url': link,
                            'judul': judul, 
                            **detail
                        }
                        results.append(InsertOne(data))
                        total_documents += 1
                        print(f'✅ {judul[:30]}... ({total_documents}/{MAX_DOCUMENTS})')

            # Bulk insert (jauh lebih cepat)
            if results:
                try:
                    print(f'📝 Menulis {len(results)} dokumen ke MongoDB...')
                    result = collections.bulk_write(results, ordered=False)
                    print(f'✅ Sukses insert {result.inserted_count} dokumen')
                except Exception as e:
                    print(f'❌ MongoDB write failed: {e}')
            else:
                print('⚠️  Tidak ada informasi valid di-halaman ini didapat')        

        except Exception as e:
            print(f"❌ Terjadi kesalahan pada halaman {page}: {e}")
            break
            
    if total_documents >= MAX_DOCUMENTS:
        print(f'\n✅ Mencapai limit maksimum: {MAX_DOCUMENTS} Documents')

if __name__ == "__main__":
    crawl_cnbc_keyword()
