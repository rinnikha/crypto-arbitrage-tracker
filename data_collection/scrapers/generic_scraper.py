import requests
from bs4 import BeautifulSoup
import time
import random


from core.dto import P2POrderDTO
from data_collection.api_clients import BaseCollector

class GenericScraper(BaseCollector):
    """Generic web scraper for exchanges without APIs."""
    
    def __init__(self, exchange_name, base_url, p2p_url=None):
        self.exchange_name = exchange_name
        self.base_url = base_url
        self.p2p_url = p2p_url or base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
    
    def _make_request(self, url, params=None):
        """Make a request to the website with proper headers and random delay."""
        # Add a small random delay to avoid rate limiting
        time.sleep(random.uniform(1, 3))
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Error making request to {url}: {e}")
            return None
    
    def fetch_p2p_from_fragment(self, asset):
        """
        Scrape P2P order data specifically from Fragment platform.
        
        Args:
            asset: Asset symbol (only TON supported)
            
        Returns:
            List of raw P2P orders with all available fields
        """
        if asset != "TON":
            return []
        
        url = "https://fragment.com/exchange/TONCOIN"
        response = self._make_request(url)
        
        if not response:
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        raw_orders = []
        
        # Find all buy orders (cards with class 'exchange_item')
        buy_listings = soup.select('.exchange_item')
        
        for listing in buy_listings:
            try:
                # Extract order details
                price_element = listing.select_one('.exchange_rate')
                price_text = price_element.text.strip() if price_element else "0"
                price = float(price_text.replace('$', '').replace(',', ''))
                
                amount_element = listing.select_one('.exchange_item_available')
                amount_text = amount_element.text.strip() if amount_element else "0 TON"
                available_amount = float(amount_text.split()[0].replace(',', ''))
                
                # Extract user info if available
                user_element = listing.select_one('.exchange_item_addr')
                user_name = user_element.text.strip() if user_element else "Fragment User"
                
                # Extract order ID if available
                order_id_element = listing.get('data-id')
                order_id = order_id_element if order_id_element else f"fragment-{len(raw_orders)}"
                
                # Determine if this is a buy or sell order
                # On Fragment, the classes or context may indicate buy/sell
                order_type_element = listing.select_one('.exchange_item_type')
                order_type = "BUY" if order_type_element and "buy" in order_type_element.text.lower() else "SELL"
                
                # Extract payment methods if available
                payment_methods_element = listing.select_one('.exchange_item_payments')
                payment_methods = []
                if payment_methods_element:
                    method_elements = payment_methods_element.select('.payment_icon')
                    for method in method_elements:
                        method_name = method.get('title', '').strip()
                        if method_name:
                            payment_methods.append(method_name)
                
                raw_order = {
                    'price': price,
                    'available_amount': available_amount,
                    'order_type': order_type,
                    'order_id': order_id,
                    'user_name': user_name,
                    'payment_methods': payment_methods
                }
                
                raw_orders.append(raw_order)
            except Exception as e:
                print(f"Error parsing Fragment listing: {e}")
                continue
        
        return raw_orders
    
    def fetch_p2p_orders(self, asset):
        pass
    
    def fetch_spot_pairs(self, base_asset = None, quote_asset = None):
        pass
    
    def fetch_available_amount(self, asset, order_type):
        pass