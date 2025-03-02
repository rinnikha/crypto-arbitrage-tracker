# analysis/liquidity.py
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
from collections import defaultdict

from data_storage.repositories import PricePointRepository
from core.models import PricePoint

class LiquidityAnalyzer:
    """Analyzer for liquidity trends across exchanges."""
    
    def __init__(self, price_repository: PricePointRepository):
        self.repository = price_repository
    
    def analyze_liquidity(self, asset: str, exchange: str, 
                         time_range: int = 24, interval: int = 1) -> Dict[str, Any]:
        """
        Analyze liquidity trends for an asset on a specific exchange.
        
        Args:
            asset: Asset symbol
            exchange: Exchange name
            time_range: Time range in hours (default: 24)
            interval: Interval in hours (default: 1)
            
        Returns:
            Dictionary with liquidity analysis
        """
        # Calculate time boundaries
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=time_range)
        
        # Get price points for both BUY and SELL orders
        buy_points = self.repository.get_price_history(
            asset_symbol=asset,
            exchange_name=exchange,
            order_type="BUY",
            market_type="P2P",
            start_time=start_time,
            end_time=end_time
        )
        
        sell_points = self.repository.get_price_history(
            asset_symbol=asset,
            exchange_name=exchange,
            order_type="SELL",
            market_type="P2P",
            start_time=start_time,
            end_time=end_time
        )
        
        # Group by time intervals
        buy_by_interval = self._group_by_interval(buy_points, start_time, end_time, interval)
        sell_by_interval = self._group_by_interval(sell_points, start_time, end_time, interval)
        
        # Calculate liquidity metrics
        result = {
            "asset": asset,
            "exchange": exchange,
            "time_range": time_range,
            "interval": interval,
            "intervals": [],
            "buy_liquidity": [],
            "sell_liquidity": [],
            "total_liquidity": [],
            "liquidity_ratio": []
        }
        
        for i, timestamp in enumerate(buy_by_interval.keys()):
            buy_liquidity = sum(p.available_amount or 0 for p in buy_by_interval[timestamp])
            sell_liquidity = sum(p.available_amount or 0 for p in sell_by_interval[timestamp])
            total_liquidity = buy_liquidity + sell_liquidity
            
            # Calculate ratio (avoid division by zero)
            ratio = buy_liquidity / sell_liquidity if sell_liquidity > 0 else float('inf')
            
            result["intervals"].append(timestamp.isoformat())
            result["buy_liquidity"].append(buy_liquidity)
            result["sell_liquidity"].append(sell_liquidity)
            result["total_liquidity"].append(total_liquidity)
            result["liquidity_ratio"].append(ratio if ratio != float('inf') else None)
        
        return result
    
    def compare_exchange_liquidity(self, asset: str, 
                                 exchanges: List[str]) -> Dict[str, Any]:
        """
        Compare liquidity across multiple exchanges.
        
        Args:
            asset: Asset symbol
            exchanges: List of exchange names
            
        Returns:
            Dictionary with liquidity comparison
        """
        result = {
            "asset": asset,
            "exchanges": exchanges,
            "buy_liquidity": {},
            "sell_liquidity": {},
            "total_liquidity": {},
            "average_price": {}
        }
        
        for exchange in exchanges:
            # Get latest prices
            latest_prices = self.repository.get_latest_prices(
                asset_symbol=asset,
                market_type="P2P"
            )
            
            # Filter by exchange
            exchange_prices = [p for p in latest_prices if p.exchange.name == exchange]
            
            # Calculate metrics
            buy_prices = [p for p in exchange_prices if p.order_type == "BUY"]
            sell_prices = [p for p in exchange_prices if p.order_type == "SELL"]
            
            buy_liquidity = sum(p.available_amount or 0 for p in buy_prices)
            sell_liquidity = sum(p.available_amount or 0 for p in sell_prices)
            total_liquidity = buy_liquidity + sell_liquidity
            
            # Calculate average prices (weighted by available amount)
            buy_avg = self._weighted_average(buy_prices) if buy_prices else None
            sell_avg = self._weighted_average(sell_prices) if sell_prices else None
            
            result["buy_liquidity"][exchange] = buy_liquidity
            result["sell_liquidity"][exchange] = sell_liquidity
            result["total_liquidity"][exchange] = total_liquidity
            result["average_price"][exchange] = {
                "buy": buy_avg,
                "sell": sell_avg
            }
        
        return result
    
    def _group_by_interval(self, price_points: List[PricePoint], 
                         start_time: datetime, end_time: datetime, 
                         interval: int) -> Dict[datetime, List[PricePoint]]:
        """
        Group price points by time interval.
        
        Args:
            price_points: List of price points
            start_time: Start time
            end_time: End time
            interval: Interval in hours
            
        Returns:
            Dictionary with price points grouped by interval
        """
        result = defaultdict(list)
        
        # Generate interval timestamps
        intervals = []
        current = start_time
        while current <= end_time:
            intervals.append(current)
            current += timedelta(hours=interval)
        
        # Assign each price point to the nearest interval
        for point in price_points:
            timestamp = point.snapshot.timestamp
            closest_interval = min(intervals, key=lambda x: abs(x - timestamp))
            result[closest_interval].append(point)
        
        # Ensure all intervals are represented
        for interval_time in intervals:
            if interval_time not in result:
                result[interval_time] = []
        
        return dict(sorted(result.items()))
    
    def _weighted_average(self, price_points: List[PricePoint]) -> float:
        """
        Calculate weighted average price based on available amount.
        
        Args:
            price_points: List of price points
            
        Returns:
            Weighted average price
        """
        total_amount = sum(p.available_amount or 0 for p in price_points)
        if total_amount == 0:
            return sum(p.price for p in price_points) / len(price_points) if price_points else 0
        
        weighted_sum = sum((p.price * (p.available_amount or 0)) for p in price_points)
        return weighted_sum / total_amount if total_amount > 0 else 0