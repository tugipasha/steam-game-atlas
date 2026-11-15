# Steam Game Atlas

Steam oyun kataloğunu GitHub Actions üzerinde parça parça toplamak için proje.

## Kurulum

1. GitHub deposunda **Settings → Secrets and variables → Actions → New repository secret** yolunu aç.
2. Secret adı: `STEAM_API_KEY`; değer: Steam Web API anahtarın.
3. **Actions** sekmesinden **Steam catalog collector** workflow'unu seç.
4. **Run workflow** ile ilk çalışmayı başlat.

Workflow her 6 saatte bir çalışır. Her çalışmada en fazla 7.000 oyun ayrıntısı çekmeyi dener; checkpoint ve sıkıştırılmış veri parçaları `data` branch'inde saklanır. Böylece Actions oturumu bittiğinde sonraki çalışma kaldığı yerden devam eder.

## Veri dosyaları

- `catalog.jsonl.gz`: Steam AppID kataloğu
- `checkpoint.json`: sıradaki katalog indeksi ve başarısız AppID'ler
- `chunks/games_*.jsonl.gz`: toplanan oyun kayıtları

Veri branch'i kod deposundan ayrı tutulur. Steam API anahtarını asla kaynak koda yazma.

## Sınırlar ve notlar

- 150.000, katalogdan hedeflenen azami AppID sayısıdır; bunların tamamı mağazada gerçek oyun olmayabilir veya ayrıntı endpoint'inden veri vermeyebilir.
- Store appdetails çağrıları hız sınırlamasına takılabilir. Kod yeniden deneme ve checkpoint uygular.
- 7.000 kayıt × 1,5 saniye yaklaşık 2 saat 55 dakika ham bekleme süresidir. GitHub Actions tek işi 6 saatle sınırlar; workflow güvenlik payı bırakır.
- İlk çalıştırma kataloğu indirir. Sonraki çalıştırmalar veri branch'indeki checkpoint'i kullanır.
- Bu workflow UMAP/embedding haritasını henüz üretmez; önce ham veri toplama tamamlanır.
