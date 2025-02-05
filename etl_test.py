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