# api/app.py
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
from datetime import datetime, timedelta

from core.models import Asset, Exchange, P2POrder, SpotPair
from data_storage.repositories import P2PRepository, SpotRepository
from analysis.opportunities import OpportunityFinder
from analysis.liquidity import LiquidityAnalyzer
from config.settings import UI_HOST, UI_PORT

def create_api(repositories):
    """Create the Flask API application."""
    app = Flask(__name__)
    CORS(app)  # Enable CORS for all routes
    
    p2p_repo = repositories["p2p_repo"]
    spot_repo = repositories["spot_repo"]
    opportunity_finder = OpportunityFinder(p2p_repo)
    liquidity_analyzer = LiquidityAnalyzer(p2p_repo)
    
    @app.route('/api/status', methods=['GET'])
    def status():
        """API status endpoint."""
        return jsonify({
            "status": "online",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat()
        })
    
    @app.route('/api/exchanges', methods=['GET'])
    def exchanges():
        """Get all exchanges."""
        exchanges_list = [
            {
                "id": exchange.id,
                "name": exchange.name,
                "base_url": exchange.base_url
            }
            for exchange in p2p_repo.db.query(Exchange).all()
        ]
        return jsonify(exchanges_list)
    
    @app.route('/api/assets', methods=['GET'])
    def assets():
        """Get all assets."""
        assets_list = [
            {
                "id": asset.id,
                "symbol": asset.symbol,
                "name": asset.name
            }
            for asset in p2p_repo.db.query(Asset).all()
        ]
        return jsonify(assets_list)
    
    @app.route('/api/p2p/latest', methods=['GET'])
    def p2p_latest():
        """Get latest P2P snapshot data."""
        asset = request.args.get('asset', 'USDT')
        exchange = request.args.get('exchange', None)
        order_type = request.args.get('order_type', None)
        
        # Get latest snapshot
        latest_snapshot = p2p_repo.get_latest_snapshot()
        if not latest_snapshot:
            return jsonify({"error": "No snapshots available"})
        
        # Build query
        query = p2p_repo.db.query(P2POrder).join(Exchange).join(Asset).filter(
            P2POrder.snapshot_id == latest_snapshot.id,
            Asset.symbol == asset
        )
        
        if exchange:
            query = query.filter(Exchange.name == exchange)
        
        if order_type:
            query = query.filter(P2POrder.order_type == order_type)
        
        orders = query.all()
        
        # Format results
        result = {
            "snapshot_id": latest_snapshot.id,
            "timestamp": latest_snapshot.timestamp.isoformat(),
            "orders": [
                {
                    "id": order.id,
                    "exchange": order.exchange.name,
                    "asset": order.asset.symbol,
                    "price": order.price,
                    "order_type": order.order_type,
                    "available_amount": order.available_amount,
                    "min_amount": order.min_amount,
                    "max_amount": order.max_amount,
                    "order_id": order.order_id,
                    "user_name": order.user_name,
                    "completion_rate": order.completion_rate
                }
                for order in orders
            ]
        }
        
        return jsonify(result)
    
    @app.route('/api/spot/latest', methods=['GET'])
    def spot_latest():
        """Get latest spot snapshot data."""
        base_asset = request.args.get('base_asset', None)
        quote_asset = request.args.get('quote_asset', 'USDT')
        exchange = request.args.get('exchange', None)
        
        # Get latest snapshot
        latest_snapshot = spot_repo.get_latest_snapshot()
        if not latest_snapshot:
            return jsonify({"error": "No snapshots available"})
        
        # Build query
        query = spot_repo.db.query(SpotPair).join(Exchange)
        
        query = query.filter(SpotPair.snapshot_id == latest_snapshot.id)
        
        if base_asset:
            query = query.join(Asset, SpotPair.base_asset_id == Asset.id)
            query = query.filter(Asset.symbol == base_asset)
        
        if quote_asset:
            query = query.join(Asset, SpotPair.quote_asset_id == Asset.id)
            query = query.filter(Asset.symbol == quote_asset)
        
        if exchange:
            query = query.filter(Exchange.name == exchange)
        
        pairs = query.all()
        
        # Format results
        result = {
            "snapshot_id": latest_snapshot.id,
            "timestamp": latest_snapshot.timestamp.isoformat(),
            "pairs": [
                {
                    "id": pair.id,
                    "exchange": pair.exchange.name,
                    "symbol": pair.symbol,
                    "base_asset": pair.base_asset.symbol,
                    "quote_asset": pair.quote_asset.symbol,
                    "price": pair.price,
                    "bid_price": pair.bid_price,
                    "ask_price": pair.ask_price,
                    "volume_24h": pair.volume_24h
                }
                for pair in pairs
            ]
        }
        
        return jsonify(result)
    
    @app.route('/api/opportunities', methods=['GET'])
    def opportunities():
        """Get arbitrage opportunities."""
        asset = request.args.get('asset', 'USDT')
        min_profit = float(request.args.get('min_profit', 2.0))
        
        opportunities_list = opportunity_finder.find_opportunities(
            asset=asset,
            min_profit_percentage=min_profit
        )
        
        result = [
            {
                "buy_exchange": opp.buy_exchange,
                "sell_exchange": opp.sell_exchange,
                "asset": opp.asset,
                "buy_price": opp.buy_price,
                "sell_price": opp.sell_price,
                "available_amount": opp.available_amount,
                "transfer_method": opp.transfer_method,
                "transfer_fee": opp.transfer_fee,
                "potential_profit": opp.potential_profit,
                "profit_percentage": opp.profit_percentage,
                "timestamp": opp.timestamp.isoformat()
            }
            for opp in opportunities_list
        ]
        
        return jsonify(result)
    
    @app.route('/api/liquidity/analysis', methods=['GET'])
    def liquidity_analysis():
        """Get liquidity analysis."""
        exchange = request.args.get('exchange', 'Binance')
        asset = request.args.get('asset', 'USDT')
        time_window = int(request.args.get('time_window', 24))
        
        analysis = liquidity_analyzer.analyze_order_changes(
            exchange_name=exchange,
            asset_symbol=asset,
            time_window=time_window
        )
        
        return jsonify(analysis)
    
    return app

def run_api(app, host=UI_HOST, port=UI_PORT):
    """Run the Flask API application."""
    app.run(host=host, port=port)