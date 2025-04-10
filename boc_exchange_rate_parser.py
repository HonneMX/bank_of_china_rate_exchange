import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import os
import time
import logging
from typing import Optional, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('boc_exchange_rate.log'),
        logging.StreamHandler()
    ]
)

class ExchangeRateParser:
    def __init__(self):
        self.base_url = 'https://www.bankofchina.com/sourcedb/whpj/enindex_1619.html'
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        self.data_dir = 'data'
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.output_file = f'{self.data_dir}/exchange_rates.csv'

    def get_exchange_rates(self) -> Optional[Dict]:
        """Fetch USD exchange rates with retry mechanism"""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(self.base_url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table', {'width': '600', 'cellpadding': '5', 'cellspacing': '1'})
                
                if not table:
                    raise Exception("Exchange rate table not found")
                
                for row in table.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if cells and cells[0].text.strip() == 'USD':
                        return {
                            'Currency Name': 'USD',
                            'Buying Rate': float(cells[1].text.strip() or 0) / 100,
                            'Cash Buying Rate': float(cells[2].text.strip() or 0) / 100,
                            'Selling Rate': float(cells[3].text.strip() or 0) / 100,
                            'Cash Selling Rate': float(cells[4].text.strip() or 0) / 100,
                            'Middle Rate': float(cells[5].text.strip() or 0) / 100,
                            'Pub Time': cells[6].text.strip(),
                            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                
                raise Exception("USD rate not found in the table")
                
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    logging.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logging.error("Max retries reached. Giving up.")
                    return None

    def save_to_csv(self, data: Dict) -> None:
        """Save exchange rate data to CSV file"""
        if not data:
            return
        
        os.makedirs(self.data_dir, exist_ok=True)
        file_exists = os.path.isfile(self.output_file)
        
        with open(self.output_file, 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'Currency Name',
                'Buying Rate',
                'Cash Buying Rate',
                'Selling Rate',
                'Cash Selling Rate',
                'Middle Rate',
                'Pub Time',
                'Timestamp'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='\t')
            
            if not file_exists:
                writer.writeheader()
            
            # Format all numeric values to 4 decimal places
            formatted_data = {
                'Currency Name': data['Currency Name'],
                'Buying Rate': f"{data['Buying Rate']:.4f}",
                'Cash Buying Rate': f"{data['Cash Buying Rate']:.4f}",
                'Selling Rate': f"{data['Selling Rate']:.4f}",
                'Cash Selling Rate': f"{data['Cash Selling Rate']:.4f}",
                'Middle Rate': f"{data['Middle Rate']:.4f}",
                'Pub Time': data['Pub Time'],
                'Timestamp': data['Timestamp']
            }
            writer.writerow(formatted_data)
        
        logging.info(f"Exchange rate data saved to {self.output_file}")

    def run(self) -> None:
        """Main execution method"""
        logging.info("Fetching USD exchange rate from Bank of China...")
        exchange_data = self.get_exchange_rates()
        
        if exchange_data:
            logging.info(f"USD Exchange Rate (Middle Rate): {exchange_data['Middle Rate']}")
            self.save_to_csv(exchange_data)
        else:
            logging.error("Failed to fetch exchange rate data")

def main():
    parser = ExchangeRateParser()
    parser.run()

if __name__ == "__main__":
    main() 