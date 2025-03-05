# analysis/opportunities.py
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from data_storage.repositories import P2PRepository
from core.utils import calculate_arbitrage_profit
from config.exchanges import TRANSFER_METHODS

@dataclass
class ArbitrageOpportunity:
    """Data class for arbitrage opportunity."""
    buy_exchange: str
    sell_exchange: str
    asset: str
    buy_price: float
    sell_price: float
    available_amount: float
    transfer_method: str
    transfer_fee: float
    potential_profit: float
    profit_percentage: float
    timestamp: datetime

class OpportunityFinder:
    """Finder for arbitrage opportunities across exchanges."""
    
    def __init__(self, price_repository: P2PRepository):
        self.repository = price_repository
    
    def find_opportunities(self, asset: str, min_profit_percentage: float = 2.0,
                         max_results: int = 10) -> List[ArbitrageOpportunity]:
        """
        Find arbitrage opportunities for an asset.
        
        Args:
            asset: Asset symbol
            min_profit_percentage: Minimum profit percentage (default: 2.0)
            max_results: Maximum number of results (default: 10)
            
        Returns:
            List of ArbitrageOpportunity objects
        """
        # Get latest prices for all exchanges
        prices = self.repository.get_latest_prices(
            asset_symbol=asset,
            market_type="P2P"
        )
        
        if not prices:
            return []
        
        # Group by exchange and order type
        prices_by_exchange = {}
        timestamp = None
        
        for price in prices:
            exchange_name = price.exchange.name
            if exchange_name not in prices_by_exchange:
                prices_by_exchange[exchange_name] = {"BUY": [], "SELL": []}
            
            prices_by_exchange[exchange_name][price.order_type].append(price)
            
            # Get snapshot timestamp
            if not timestamp and price.snapshot:
                timestamp = price.snapshot.timestamp
        
        # Find opportunities
        opportunities = []
        
        for buy_exchange, buy_data in prices_by_exchange.items():
            for sell_exchange, sell_data in prices_by_exchange.items():
                if buy_exchange == sell_exchange:
                    continue
                
                # Check if there are any buy or sell orders
                if not buy_data["BUY"] or not sell_data["SELL"]:
                    continue
                
                # Find best buy price (lowest)
                best_buy = min(buy_data["BUY"], key=lambda x: x.price)
                
                # Find best sell price (highest)
                best_sell = max(sell_data["SELL"], key=lambda x: x.price)
                
                # Calculate maximum transferable amount
                max_amount = min(
                    best_buy.available_amount or 0,
                    best_sell.available_amount or 0
                )
                
                # Check if there's enough liquidity
                if max_amount <= 0:
                    continue
                
                # Get transfer method and fee
                transfer_key = f"{buy_exchange.lower()}_to_{sell_exchange.lower()}"
                if transfer_key not in TRANSFER_METHODS:
                    # Try reverse
                    transfer_key = f"{sell_exchange.lower()}_to_{buy_exchange.lower()}"
                
                # Skip if no transfer method found
                if transfer_key not in TRANSFER_METHODS:
                    continue
                
                transfer_info = TRANSFER_METHODS[transfer_key]
                transfer_fee = transfer_info["fixed_fee"]
                
                # Calculate profit
                profit_calc = calculate_arbitrage_profit(
                    buy_price=best_buy.price,
                    sell_price=best_sell.price,
                    amount=max_amount,
                    transfer_fee=transfer_fee
                )
                
                # Check if profitable
                if profit_calc["profit_percent"] >= min_profit_percentage:
                    opportunity = ArbitrageOpportunity(
                        buy_exchange=buy_exchange,
                        sell_exchange=sell_exchange,
                        asset=asset,
                        buy_price=best_buy.price,
                        sell_price=best_sell.price,
                        available_amount=max_amount,
                        transfer_method=transfer_info["network"],
                        transfer_fee=transfer_fee,
                        potential_profit=profit_calc["profit"],
                        profit_percentage=profit_calc["profit_percent"],
                        timestamp=timestamp or datetime.now()
                    )
                    
                    opportunities.append(opportunity)
        
        # Sort by profit percentage (descending)
        sorted_opportunities = sorted(
            opportunities,
            key=lambda x: x.profit_percentage,
            reverse=True
        )
        
        # Return top results
        return sorted_opportunities[:max_results]
    
    def calculate_opportunity_details(self, opportunity: ArbitrageOpportunity, 
                                     amount: Optional[float] = None) -> Dict[str, Any]:
        """
        Calculate detailed metrics for an arbitrage opportunity.
        
        Args:
            opportunity: ArbitrageOpportunity object
            amount: Custom amount to calculate for (optional)
            
        Returns:
            Dictionary with detailed metrics
        """
        # Determine amount
        if amount is None or amount <= 0 or amount > opportunity.available_amount:
            amount = opportunity.available_amount
        
        # Calculate profit
        profit_calc = calculate_arbitrage_profit(
            buy_price=opportunity.buy_price,
            sell_price=opportunity.sell_price,
            amount=amount,
            transfer_fee=opportunity.transfer_fee
        )
        
        return {
            "buy_exchange": opportunity.buy_exchange,
            "sell_exchange": opportunity.sell_exchange,
            "asset": opportunity.asset,
            "amount": amount,
            "buy_price": opportunity.buy_price,
            "sell_price": opportunity.sell_price,
            "price_difference": opportunity.sell_price - opportunity.buy_price,
            "price_difference_percentage": (
                (opportunity.sell_price - opportunity.buy_price) / opportunity.buy_price * 100
            ),
            "buy_cost": profit_calc["buy_cost"],
            "sell_revenue": profit_calc["sell_revenue"],
            "transfer_method": opportunity.transfer_method,
            "transfer_fee": opportunity.transfer_fee,
            "net_profit": profit_calc["profit"],
            "profit_percentage": profit_calc["profit_percent"],
            "timestamp": opportunity.timestamp.isoformat(),
            "roi_per_hour": profit_calc["profit_percent"] / 2,  # Assuming 2 hours to complete
            "roi_per_day": profit_calc["profit_percent"] * 12  # Assuming 2 hours to complete
        }