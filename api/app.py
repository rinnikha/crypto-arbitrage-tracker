"""
RESTful API for the Crypto Arbitrage Tracker application.
"""
import logging
import time
import json
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, jsonify, request, Response, Blueprint, current_app
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from config.manager import get_config
from core.errors import AppError, ApiError
from core.dto import SnapshotResultDTO, ArbitrageOpportunityDTO
from data_storage.repositories import P2PRepository, SpotRepository
from analysis.opportunities import OpportunityFinder
from analysis.liquidity import LiquidityAnalyzer

logger = logging.getLogger(__name__)


# Define API response codes
class ApiStatus:
    SUCCESS = "success"
    ERROR = "error"


def create_api(repositories: Dict[str, Any]) -> Flask:
    """
    Create the Flask API application.

    Args:
        repositories: Dictionary of repository instances

    Returns:
        Flask application
    """
    # Get config
    config = get_config()

    # Create Flask app
    app = Flask(__name__)

    # Configure app
    app.config["JSON_SORT_KEYS"] = False
    app.config["JSONIFY_PRETTYPRINT_REGULAR"] = config.get_bool("API_PRETTY_JSON", False)

    # Enable CORS
    cors_origins = config.get("API_CORS_ORIGINS", "*")
    CORS(app, resources={r"/api/*": {"origins": cors_origins}})

    # Get repositories
    p2p_repo = repositories.get("p2p_repo")
    spot_repo = repositories.get("spot_repo")

    if not p2p_repo or not spot_repo:
        raise ValueError("P2P and Spot repositories are required")

    # Initialize analyzers
    opportunity_finder = OpportunityFinder(p2p_repo)
    liquidity_analyzer = LiquidityAnalyzer(p2p_repo)

    # Store analyzers in app config for access in routes
    app.config["repositories"] = repositories
    app.config["analyzers"] = {
        "opportunity_finder": opportunity_finder,
        "liquidity_analyzer": liquidity_analyzer
    }

    # Register error handlers
    @app.errorhandler(Exception)
    def handle_exception(e):
        """Handle all exceptions."""
        return _handle_error(e)

    # Create API blueprint
    api_bp = Blueprint("api", __name__, url_prefix="/api")

    # Register routes

    @api_bp.route('/status', methods=['GET'])
    def status():
        """API status endpoint."""
        return _api_response({
            "status": "online",
            "version": "1.0.0",
            "timestamp": datetime.now().isoformat()
        })

    @api_bp.route('/exchanges', methods=['GET'])
    def exchanges():
        """Get all exchanges."""
        try:
            from core.models import Exchange

            exchanges_list = [
                {
                    "id": exchange.id,
                    "name": exchange.name,
                    "base_url": exchange.base_url
                }
                for exchange in p2p_repo.db.query(Exchange).all()
            ]

            return _api_response(exchanges_list)
        except Exception as e:
            logger.exception(f"Error getting exchanges: {e}")
            raise ApiError(f"Failed to get exchanges: {str(e)}")

    @api_bp.route('/assets', methods=['GET'])
    def assets():
        """Get all assets."""
        try:
            from core.models import Asset

            assets_list = [
                {
                    "id": asset.id,
                    "symbol": asset.symbol,
                    "name": asset.name
                }
                for asset in p2p_repo.db.query(Asset).all()
            ]

            return _api_response(assets_list)
        except Exception as e:
            logger.exception(f"Error getting assets: {e}")
            raise ApiError(f"Failed to get assets: {str(e)}")

    @api_bp.route('/fiats', methods=['GET'])
    def fiats():
        """Get all fiat currencies."""
        try:
            from core.models import Fiat

            fiats_list = [
                {
                    "id": fiat.id,
                    "code": fiat.code,
                    "name": fiat.name
                }
                for fiat in p2p_repo.db.query(Fiat).all()
            ]

            return _api_response(fiats_list)
        except Exception as e:
            logger.exception(f"Error getting fiats: {e}")
            raise ApiError(f"Failed to get fiats: {str(e)}")

    @api_bp.route('/p2p/latest', methods=['GET'])
    def p2p_latest():
        """Get latest P2P snapshot data."""
        try:
            asset = request.args.get('asset', 'USDT')
            exchange = request.args.get('exchange')
            order_type = request.args.get('order_type')

            # Get latest snapshot
            latest_snapshot = p2p_repo.get_latest_snapshot()
            if not latest_snapshot:
                return _api_response({"error": "No snapshots available"}, status_code=404)

            # Get orders
            orders = p2p_repo.get_orders_by_snapshot(
                snapshot_id=latest_snapshot.id,
                exchange_name=exchange,
                asset_symbol=asset,
                order_type=order_type
            )

            # Format results
            result = {
                "snapshot_id": latest_snapshot.id,
                "timestamp": latest_snapshot.timestamp.isoformat(),
                "orders": [
                    {
                        "id": order.id,
                        "exchange": order.exchange.name,
                        "asset": order.asset.symbol,
                        "fiat": order.fiat.code,
                        "price": order.price,
                        "order_type": order.order_type,
                        "available_amount": order.available_amount,
                        "min_amount": order.min_amount,
                        "max_amount": order.max_amount,
                        "payment_methods": order.payment_methods,
                        "order_id": order.order_id,
                        "user_name": order.user_name,
                        "completion_rate": order.completion_rate
                    }
                    for order in orders
                ]
            }

            return _api_response(result)
        except Exception as e:
            logger.exception(f"Error getting latest P2P data: {e}")
            raise ApiError(f"Failed to get latest P2P data: {str(e)}")

    @api_bp.route('/spot/latest', methods=['GET'])
    def spot_latest():
        """Get latest spot snapshot data."""
        try:
            base_asset = request.args.get('base_asset')
            quote_asset = request.args.get('quote_asset', 'USDT')
            exchange = request.args.get('exchange')

            # Get latest snapshot
            latest_snapshot = spot_repo.get_latest_snapshot()
            if not latest_snapshot:
                return _api_response({"error": "No snapshots available"}, status_code=404)

            # Get pairs
            pairs = spot_repo.get_pairs_by_snapshot(
                snapshot_id=latest_snapshot.id,
                exchange_name=exchange,
                base_asset_symbol=base_asset,
                quote_asset_symbol=quote_asset
            )

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
                        "volume_24h": pair.volume_24h,
                        "high_24h": pair.high_24h,
                        "low_24h": pair.low_24h
                    }
                    for pair in pairs
                ]
            }

            return _api_response(result)
        except Exception as e:
            logger.exception(f"Error getting latest spot data: {e}")
            raise ApiError(f"Failed to get latest spot data: {str(e)}")

    @api_bp.route('/opportunities', methods=['GET'])
    def opportunities():
        """Get arbitrage opportunities."""
        try:
            asset = request.args.get('asset', 'USDT')
            min_profit = float(request.args.get('min_profit', 2.0))
            max_results = int(request.args.get('max_results', 10))

            opportunities_list = opportunity_finder.find_opportunities(
                asset=asset,
                min_profit_percentage=min_profit,
                max_results=max_results
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
                    "timestamp": opp.timestamp.isoformat(),
                    "hourly_roi": opp.get_hourly_roi(),
                    "daily_roi": opp.get_daily_roi()
                }
                for opp in opportunities_list
            ]

            return _api_response(result)
        except Exception as e:
            logger.exception(f"Error finding opportunities: {e}")
            raise ApiError(f"Failed to find opportunities: {str(e)}")

    @api_bp.route('/liquidity/analysis', methods=['GET'])
    def liquidity_analysis():
        """Get liquidity analysis."""
        try:
            exchange = request.args.get('exchange', 'Binance')
            asset = request.args.get('asset', 'USDT')
            time_window = int(request.args.get('time_window', 24))

            analysis = liquidity_analyzer.analyze_order_changes(
                exchange_name=exchange,
                asset_symbol=asset,
                time_window=time_window
            )

            return _api_response(analysis)
        except Exception as e:
            logger.exception(f"Error analyzing liquidity: {e}")
            raise ApiError(f"Failed to analyze liquidity: {str(e)}")

    @api_bp.route('/snapshots/<snapshot_type>', methods=['GET'])
    def get_snapshots(snapshot_type):
        """
        Get snapshots of a specific type.

        Args:
            snapshot_type: Type of snapshot (p2p or spot)
        """
        try:
            # Get query parameters
            limit = int(request.args.get('limit', 10))
            offset = int(request.args.get('offset', 0))

            if snapshot_type == 'p2p':
                # Get P2P snapshots
                from core.models import P2PSnapshot

                snapshots = p2p_repo.db.query(P2PSnapshot) \
                    .order_by(P2PSnapshot.timestamp.desc()) \
                    .limit(limit) \
                    .offset(offset) \
                    .all()

                result = [
                    {
                        "id": snapshot.id,
                        "timestamp": snapshot.timestamp.isoformat(),
                        "orders_count": len(snapshot.orders)
                    }
                    for snapshot in snapshots
                ]
            elif snapshot_type == 'spot':
                # Get spot snapshots
                from core.models import SpotSnapshot

                snapshots = spot_repo.db.query(SpotSnapshot) \
                    .order_by(SpotSnapshot.timestamp.desc()) \
                    .limit(limit) \
                    .offset(offset) \
                    .all()

                result = [
                    {
                        "id": snapshot.id,
                        "timestamp": snapshot.timestamp.isoformat(),
                        "pairs_count": len(snapshot.pairs)
                    }
                    for snapshot in snapshots
                ]
            else:
                return _api_response(
                    {"error": f"Invalid snapshot type: {snapshot_type}"},
                    status_code=400
                )

            return _api_response(result)
        except Exception as e:
            logger.exception(f"Error getting snapshots: {e}")
            raise ApiError(f"Failed to get snapshots: {str(e)}")

    @api_bp.route('/snapshot/<int:snapshot_id>/<snapshot_type>', methods=['GET'])
    def get_snapshot(snapshot_id, snapshot_type):
        """
        Get a specific snapshot.

        Args:
            snapshot_id: Snapshot ID
            snapshot_type: Type of snapshot (p2p or spot)
        """
        try:
            if snapshot_type == 'p2p':
                # Get P2P snapshot
                snapshot = p2p_repo.get_snapshot_by_id(snapshot_id)
                if not snapshot:
                    return _api_response(
                        {"error": f"P2P snapshot {snapshot_id} not found"},
                        status_code=404
                    )

                # Get orders
                asset = request.args.get('asset')
                exchange = request.args.get('exchange')
                order_type = request.args.get('order_type')

                orders = p2p_repo.get_orders_by_snapshot(
                    snapshot_id=snapshot.id,
                    exchange_name=exchange,
                    asset_symbol=asset,
                    order_type=order_type
                )

                result = {
                    "id": snapshot.id,
                    "timestamp": snapshot.timestamp.isoformat(),
                    "orders": [
                        {
                            "id": order.id,
                            "exchange": order.exchange.name,
                            "asset": order.asset.symbol,
                            "fiat": order.fiat.code,
                            "price": order.price,
                            "order_type": order.order_type,
                            "available_amount": order.available_amount,
                            "min_amount": order.min_amount,
                            "max_amount": order.max_amount,
                            "payment_methods": order.payment_methods,
                            "order_id": order.order_id,
                            "user_name": order.user_name,
                            "completion_rate": order.completion_rate
                        }
                        for order in orders
                    ]
                }
            elif snapshot_type == 'spot':
                # Get spot snapshot
                snapshot = spot_repo.get_snapshot_by_id(snapshot_id)
                if not snapshot:
                    return _api_response(
                        {"error": f"Spot snapshot {snapshot_id} not found"},
                        status_code=404
                    )

                # Get pairs
                base_asset = request.args.get('base_asset')
                quote_asset = request.args.get('quote_asset')
                exchange = request.args.get('exchange')

                pairs = spot_repo.get_pairs_by_snapshot(
                    snapshot_id=snapshot.id,
                    exchange_name=exchange,
                    base_asset_symbol=base_asset,
                    quote_asset_symbol=quote_asset
                )

                result = {
                    "id": snapshot.id,
                    "timestamp": snapshot.timestamp.isoformat(),
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
                            "volume_24h": pair.volume_24h,
                            "high_24h": pair.high_24h,
                            "low_24h": pair.low_24h
                        }
                        for pair in pairs
                    ]
                }
            else:
                return _api_response(
                    {"error": f"Invalid snapshot type: {snapshot_type}"},
                    status_code=400
                )

            return _api_response(result)
        except Exception as e:
            logger.exception(f"Error getting snapshot: {e}")
            raise ApiError(f"Failed to get snapshot: {str(e)}")

    # Register blueprint
    app.register_blueprint(api_bp)

    return app


def run_api(app: Flask, host: str, port: int) -> None:
    """
    Run the Flask API application.

    Args:
        app: Flask application
        host: Host to listen on
        port: Port to listen on
    """
    app.run(host=host, port=port)


def _api_response(data: Any, status_code: int = 200) -> Response:
    """
    Create a standardized API response.

    Args:
        data: Response data
        status_code: HTTP status code

    Returns:
        Flask response
    """
    if status_code >= 400:
        response = {
            "status": ApiStatus.ERROR,
            "error": data if isinstance(data, dict) and "error" in data else {
                "message": "An error occurred",
                "details": data
            },
            "timestamp": datetime.now().isoformat()
        }
    else:
        response = {
            "status": ApiStatus.SUCCESS,
            "data": data,
            "timestamp": datetime.now().isoformat()
        }

    return jsonify(response), status_code


def _handle_error(e: Exception) -> Tuple[Response, int]:
    """
    Handle an error and return an appropriate response.

    Args:
        e: Exception to handle

    Returns:
        Response and status code
    """
    if isinstance(e, HTTPException):
        # Handle Flask exceptions
        status_code = e.code
        error_data = {
            "message": e.description,
            "code": e.name
        }
    elif isinstance(e, ApiError):
        # Handle API errors
        status_code = 400
        error_data = {
            "message": str(e),
            "code": e.__class__.__name__
        }
        if hasattr(e, "details") and e.details:
            error_data["details"] = e.details
    elif isinstance(e, AppError):
        # Handle application errors
        status_code = 500
        error_data = {
            "message": str(e),
            "code": e.__class__.__name__
        }
        if hasattr(e, "details") and e.details:
            error_data["details"] = e.details
    else:
        # Handle unexpected errors
        status_code = 500
        error_data = {
            "message": str(e) or "An unexpected error occurred",
            "code": "ServerError"
        }

    # Log the error
    if status_code >= 500:
        logger.exception(f"Server error: {e}")
    elif status_code >= 400:
        logger.error(f"Client error: {e}")

    return _api_response(error_data, status_code=status_code)