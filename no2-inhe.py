#Inheritance
import pandas as pd

class MarketingDataETL:
    def __init__(self, url):
        self.url = url
        self.data = None

    def extract(self):
        # Membaca file CSV menjadi DataFrame
        self.data = pd.read_csv(self.url, delimiter=';')

    def transform(self):
        # Menghapus baris dengan nilai kosong (NA)
        if self.data is not None:
            self.data.dropna(inplace=True)
            # Konversi kolom 'purchase_date' menjadi format tanggal (YYYY-MM-DD)
            self.data['purchase_date'] = pd.to_datetime(self.data['purchase_date'], format='%d/%m/%y')
            # Mengubah format kolom 'purchase_date' menjadi YYYY-MM-DD
            self.data['purchase_date'] = self.data['purchase_date'].dt.strftime('%Y-%m-%d')

    def store(self, output_file):
        # Store transformasi DataFrame menjadi file CSV 
        if self.data is not None:
            self.data.to_csv(output_file, index=False)

class TargetedMarketingETL(MarketingDataETL):
    def segment_customers(self):
        if self.data is not None:
            # Mengkategorikan tingkatan customers berdasarkan jumlah yang dieluarkan 
            bins = [0, 100, 300, 500, float('inf')]
            labels = ['Bronze', 'Silver', 'Gold', 'Platinum']
            self.data['membership_rank'] = pd.cut(self.data['amount_spent'], bins=bins, labels=labels, right=False)

# Penggunaan:
url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"
etl_processor = TargetedMarketingETL(url)
etl_processor.extract()
etl_processor.transform()
etl_processor.segment_customers()
etl_processor.store("transformed_data.csv")

# Membaca file CSV menjadi DataFrame
df = pd.read_csv("transformed_data.csv")

# Menampilkan DataFrame
print(df)
