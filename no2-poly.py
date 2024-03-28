#Polymorphism
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
            # Mengkategorikan tingkatan customers berdasarkan jumlah yang dikeluarkan 
            bins = [0, 100, 300, 500, float('inf')]
            labels = ['Bronze', 'Silver', 'Gold', 'Platinum']
            self.data['membership_rank'] = pd.cut(self.data['amount_spent'], bins=bins, labels=labels, right=False)

    def transform(self):
        # Memanggil metode transformasi kelas induk untuk melakukan transformasi awal
        super().transform()

        # Transformasi tambahan khusus untuk TargetedMarketingETL
        self.segment_customers()

class MarketingDataViewer:
    def display(self, data):
        # Menampilkan DataFrame
        print(data)

# URL sumber data
url = "https://drive.google.com/uc?id=13wg8hC7kpMSzNeS2c27dTKplRKkLgNfn"

# Proses ETL menggunakan kelas TargetedMarketingETL
etl_processor = TargetedMarketingETL(url)

# Ekstraksi, transformasi, dan segmentasi pelanggan
etl_processor.extract()
etl_processor.transform()  # Metode transform overridden
etl_processor.store("transformed_data.csv")

# Membaca data hasil transformasi ke dalam DataFrame
df = pd.read_csv("transformed_data.csv")

# Membuat objek MarketingDataViewer
viewer = MarketingDataViewer()

# Menampilkan DataFrame menggunakan Polymorphism
viewer.display(df)
