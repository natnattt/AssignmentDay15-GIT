import pandas as pd

class MarketingDataETL:
    def __init__(self, url):
        self.url = url
        self.data = None

    def extract(self):
        # Membaca file CSV menjadi DataFrame
        self.data = pd.read_csv(self.url, delimiter=';')
        print("extract Done")

    def transform(self):
        # Menghapus baris dengan nilai kosong (NA)
        if self.data is not None:
            self.data.dropna(inplace=True)
            # Mengubah kolom 'purchase_date' menjadi format tanggal (YYYY-MM-DD)
            self.data['purchase_date'] = pd.to_datetime(self.data['purchase_date'], format='%d/%m/%y')
            # Mengubah kolom 'purchase_date' menjadi YYYY-MM-DD
            self.data['purchase_date'] = self.data['purchase_date'].dt.strftime('%Y-%m-%d')
            print("transform Done")

    def store(self, output_file):
        # Store transformasi DataFrame menjadi file CSV 
        if self.data is not None:
            self.data.to_csv(output_file, index=False)
            print("Store Done")

# Penggunaan:
if __name__ == "__main__":
    # URL dari file CSV 
    url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"

    # Inisialisasi kelas ETL dengan URL
    etl_processor = MarketingDataETL(url)

    # Ekstrak data
    etl_processor.extract()

    # Transformasi data (menghapus nilai NA dan mengubah format tanggal)
    etl_processor.transform()

    # Store transformasi DataFrame menjadi file CSV
    etl_processor.store("transformed_marketing_data.csv")

# Membaca file CSV menjadi DataFrame hasil dari transformasi
df = pd.read_csv("transformed_marketing_data.csv")

# Menampilkan DataFrame
print(df)
