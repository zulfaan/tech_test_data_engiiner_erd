# Penggabungan File CSV Menggunakan Python dan Luigi

## Deskripsi
Sebagai seorang Data Engineer di Erdigma, tugas ini bertujuan untuk menggabungkan beberapa file CSV yang berasal dari Google Drive ke dalam satu file agar dapat diinput ke database. Pengolahan data ini dilakukan dengan menggunakan Python dan Luigi sebagai workflow automation.

## Ketentuan
- **Kolom yang diinginkan:**
  - `raw_date` = Tanggal
  - `amount` = Pengeluaran(IDR)
  - `description` = '' (nilai null)
  - `mp` = dari file name index 1
  - `ja_per_team` = dari file name index 0
  - `store` = dari file name index 2

- **Sumber Data:**
  - Data berasal dari beberapa file CSV dengan format nama:
    ```
    Bm Marketplace EDC_Shopee CPAS_Herbavitshop Official_<tanggal>.csv
    ```
  - File berada dalam folder `source/`

## Implementasi Kode
Kode berikut menggunakan **Luigi** untuk mengotomatisasi proses pengambilan, transformasi, dan penyimpanan data ke dalam satu file CSV terpusat.

```python
import pandas as pd
import luigi
import os 

class ExtractTransformERD(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan

    def output(self):
        return luigi.LocalTarget('output/merge_data.csv') # Menyimpan data yang diekstrak ke file CSV

    def run(self):
        # Membaca data dari file CSV
        EDC_Shopee = [
            'source/Bm Marketplace EDC_Shopee CPAS_Herbavitshop Official_22 Januari 2025.csv',
             'source/Bm Marketplace EDC_Shopee CPAS_Herbavitshop Official_23 Januari 2025.csv',
             'source/Bm Marketplace EDC_Shopee CPAS_Herbavitshop Official_24 Januari 2025.csv',
             'source/Bm Marketplace EDC_Shopee CPAS_Herbavitshop Official_25-27 Januari 2025.csv',
             'source/Bm Marketplace EDC_Shopee CPAS_Herbavitshop Official_28 Januari 2025.csv']

        EDC_Shopee_df = []

        # Menggabungkan data dari file CSV    
        for file in EDC_Shopee:
            filename = os.path.basename(file).replace('.csv', '')

            filename_parts = filename.split('_')

            if len(filename_parts) >= 3:
                ja_per_team = filename_parts[0]
                mp = filename_parts[1]
                store = filename_parts[2]
            else:
                continue

            data = pd.read_csv(file)
            data_selected = data[['Tanggal', 'Pengeluaran(IDR)']].copy()
            
            # Mengubah nama sesuai ketentuan
            data_selected.rename(columns={'Tanggal': 'raw_date', 'Pengeluaran(IDR)': 'amount'}, inplace=True)

            # Menambahkan kolom baru sesuai ketentuan
            data_selected['description'] = ''
            data_selected['mp'] = mp
            data_selected['ja_per_team'] = ja_per_team
            data_selected['store'] = store

            EDC_Shopee_df.append(data_selected)

        # Menggabungkan data dari file CSV
        EDC_Shopee_df = pd.concat(EDC_Shopee_df, ignore_index=True)

        # Menyimpan data yang diekstrak ke file CSV 
        EDC_Shopee_df.to_csv(self.output().path, index=False)

if __name__ == '__main__':
    luigi.build([ExtractTransformERD()
                ], local_scheduler=True) # Menjalankan scheduler lokal untuk mengatur eskusi tugas
```

## Penjelasan Kode
1. **Mengumpulkan File CSV:**
   - Data diambil dari folder `source/` dengan nama file yang memiliki pola tertentu.
   - Nama file digunakan untuk menentukan nilai `ja_per_team`, `mp`, dan `store`.

2. **Ekstraksi Data:**
   - Data dibaca dari CSV menggunakan `pandas.read_csv()`.
   - Hanya kolom `Tanggal` dan `Pengeluaran(IDR)` yang diambil.

3. **Transformasi Data:**
   - Kolom `Tanggal` diubah namanya menjadi `raw_date`.
   - Kolom `Pengeluaran(IDR)` diubah namanya menjadi `amount`.
   - Kolom baru `description`, `mp`, `ja_per_team`, dan `store` ditambahkan.

4. **Penggabungan Data:**
   - Semua data dari file CSV yang telah diproses digabung menggunakan `pd.concat()`.
   - Hasilnya disimpan ke dalam file `output/merge_data.csv`.

## Kesimpulan
Dengan menggunakan Luigi, proses ekstraksi dan penggabungan data CSV dapat dilakukan secara otomatis dan lebih terstruktur. Tugas ini memungkinkan data yang berasal dari berbagai sumber dikompilasi ke dalam satu dataset yang siap untuk dimasukkan ke dalam database.