# analysis/liquidity.py
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict

from data_storage.repositories import P2PRepository

class LiquidityAnalyzer:
    """Analyzer for liquidity trends across exchanges."""
    
    def __init__(self, p2p_repository: P2PRepository):
        self.repository = p2p_repository
    
    def analyze_order_changes(self, exchange_name: str, asset_symbol: str, 
                            time_window: int = 24) -> Dict[str, Any]:
        """
        Analyze changes in P2P orders over time to identify liquidity consumption.
        
        Args:
            exchange_name: Name of the exchange
            asset_symbol: Symbol of the asset
            time_window: Time window in hours (default: 24)
            
        Returns:
            Dictionary with liquidity analysis
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=time_window)
        
        # Get all snapshots in the time window
        snapshots = self.repository.db.query(P2PSnapshot).filter(
            P2PSnapshot.timestamp >= start_time,
            P2PSnapshot.timestamp <= end_time
        ).order_by(P2PSnapshot.timestamp).all()
        
        if not snapshots:
            return {"error": "No snapshots found in the specified time window"}
        
        # Get all orders for the exchange and asset in these snapshots
        orders = self.repository.db.query(P2POrder).join(Exchange).join(Asset).join(P2PSnapshot).filter(
            Exchange.name == exchange_name,
            Asset.symbol == asset_symbol,
            P2PSnapshot.id.in_([s.id for s in snapshots])
        ).all()
        
        # Group orders by order_id and snapshot timestamp
        orders_by_id = defaultdict(list)
        for order in orders:
            orders_by_id[order.order_id].append({
                "snapshot_id": order.snapshot_id,
                "timestamp": order.snapshot.timestamp,
                "available_amount": order.available_amount,
                "price": order.price,
                "order_type": order.order_type,
                "user_id": order.user_id,
                "user_name": order.user_name
            })
        
        # Sort each order's history by timestamp
        for order_id in orders_by_id:
            orders_by_id[order_id] = sorted(
                orders_by_id[order_id],
                key=lambda x: x["timestamp"]
            )
        
        # Analyze changes in available amount
        liquidity_consumed = 0
        liquidity_added = 0
        active_orders = 0
        completed_orders = 0
        orders_with_changes = []
        
        for order_id, history in orders_by_id.items():
            if len(history) <= 1:
                # Skip orders with only one snapshot
                continue
                
            # Check if the order was active in the latest snapshot
            is_active = any(h["snapshot_id"] == snapshots[-1].id for h in history)
            
            # Track changes across snapshots
            for i in range(1, len(history)):
                prev = history[i-1]
                curr = history[i]
                
                # Calculate change in available amount
                amount_change = curr["available_amount"] - prev["available_amount"]
                
                if amount_change < 0:
                    # Negative change indicates liquidity consumed
                    liquidity_consumed += abs(amount_change)
                    
                    # Record significant changes
                    if abs(amount_change) > 0.01 * prev["available_amount"]:  # 1% threshold
                        orders_with_changes.append({
                            "order_id": order_id,
                            "user_name": curr["user_name"],
                            "order_type": curr["order_type"],
                            "previous_amount": prev["available_amount"],
                            "current_amount": curr["available_amount"],
                            "change": amount_change,
                            "change_percentage": (amount_change / prev["available_amount"]) * 100,
                            "price": curr["price"],
                            "timestamp": curr["timestamp"].isoformat()
                        })
                elif amount_change > 0:
                    # Positive change indicates liquidity added
                    liquidity_added += amount_change
            
            if is_active:
                active_orders += 1
            else:
                completed_orders += 1
        
        # Calculate total liquidity in latest snapshot
        latest_snapshot = snapshots[-1]
        latest_liquidity = sum(
            order.available_amount
            for order in orders
            if order.snapshot_id == latest_snapshot.id
        )
        
        # Organize results
        return {
            "exchange": exchange_name,
            "asset": asset_symbol,
            "time_window_hours": time_window,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "snapshots_count": len(snapshots),
            "unique_orders_count": len(orders_by_id),
            "active_orders": active_orders,
            "completed_orders": completed_orders,
            "current_liquidity": latest_liquidity,
            "liquidity_consumed": liquidity_consumed,
            "liquidity_added": liquidity_added,
            "net_liquidity_change": liquidity_added - liquidity_consumed,
            "consumption_rate_per_hour": liquidity_consumed / time_window,
            "significant_changes": sorted(
                orders_with_changes,
                key=lambda x: abs(x["change"]),
                reverse=True
            )[:20]  # Top 20 significant changes
        }