# ui/dashboard.py
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd

from analysis.opportunities import OpportunityFinder
from analysis.liquidity import LiquidityAnalyzer
from data_storage.repositories import PricePointRepository, SnapshotRepository

def create_dashboard(app, repositories):
    """
    Create dashboard components.
    
    Args:
        app: Dash application
        repositories: Dictionary of repositories
        
    Returns:
        Dashboard layout
    """
    price_repo = repositories["price_repo"]
    snapshot_repo = repositories["snapshot_repo"]
    opportunity_finder = OpportunityFinder(price_repo)
    liquidity_analyzer = LiquidityAnalyzer(price_repo)
    
    # Create layout
    layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("Crypto Arbitrage Tracker", className="text-center mt-3 mb-4")
            ])
        ]),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H3("Market Overview", className="card-title"),
                        html.Div([
                            dbc.Button("Refresh", id="refresh-button", color="primary", className="me-2"),
                            html.Span(id="last-update-time")
                        ], className="d-flex justify-content-between align-items-center")
                    ]),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Asset:"),
                                dcc.Dropdown(
                                    id="asset-selector",
                                    options=[
                                        {"label": "USDT", "value": "USDT"},
                                        {"label": "BTC", "value": "BTC"},
                                        {"label": "ETH", "value": "ETH"},
                                        {"label": "TON", "value": "TON"}
                                    ],
                                    value="USDT",
                                    clearable=False
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Market Type:"),
                                dcc.Dropdown(
                                    id="market-type-selector",
                                    options=[
                                        {"label": "P2P", "value": "P2P"},
                                        {"label": "Exchange", "value": "EXCHANGE"},
                                        {"label": "All", "value": "ALL"}
                                    ],
                                    value="P2P",
                                    clearable=False
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Order Type:"),
                                dcc.Dropdown(
                                    id="order-type-selector",
                                    options=[
                                        {"label": "Buy", "value": "BUY"},
                                        {"label": "Sell", "value": "SELL"},
                                        {"label": "All", "value": "ALL"}
                                    ],
                                    value="ALL",
                                    clearable=False
                                )
                            ], width=3)
                        ], className="mb-3"),
                        
                        html.Div(id="price-comparison-cards", className="mb-4"),
                        
                        dcc.Graph(id="price-comparison-chart")
                    ])
                ], className="mb-4")
            ])
        ]),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H3("Arbitrage Opportunities", className="card-title")
                    ]),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Minimum Profit %:"),
                                dcc.Slider(
                                    id="min-profit-slider",
                                    min=0.5,
                                    max=10,
                                    step=0.5,
                                    value=2,
                                    marks={i: f"{i}%" for i in range(1, 11, 1)},
                                    className="mb-3"
                                )
                            ], width=12)
                        ]),
                        
                        html.Div(id="opportunities-table")
                    ])
                ], className="mb-4")
            ])
        ]),
        
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader([
                        html.H3("Liquidity Analysis", className="card-title")
                    ]),
                    dbc.CardBody([
                        dbc.Row([
                            dbc.Col([
                                html.Label("Exchange:"),
                                dcc.Dropdown(
                                    id="liquidity-exchange-selector",
                                    options=[
                                        {"label": "Binance", "value": "Binance"},
                                        {"label": "Bitget", "value": "Bitget"},
                                        {"label": "Bybit", "value": "Bybit"},
                                        {"label": "MEXC", "value": "MEXC"}
                                    ],
                                    value="Binance",
                                    clearable=False
                                )
                            ], width=3),
                            dbc.Col([
                                html.Label("Time Range:"),
                                dcc.Dropdown(
                                    id="liquidity-timerange-selector",
                                    options=[
                                        {"label": "Last 6 hours", "value": 6},
                                        {"label": "Last 12 hours", "value": 12},
                                        {"label": "Last 24 hours", "value": 24},
                                        {"label": "Last 48 hours", "value": 48}
                                    ],
                                    value=24,
                                    clearable=False
                                )
                            ], width=3)
                        ], className="mb-3"),
                        
                        dcc.Graph(id="liquidity-trend-chart")
                    ])
                ])
            ])
        ]),
        
        dcc.Interval(
            id="refresh-interval",
            interval=5*60*1000,  # Every 5 minutes
            n_intervals=0
        )
    ], fluid=True)
    
    # Define callbacks
    @app.callback(
        [
            Output("price-comparison-cards", "children"),
            Output("price-comparison-chart", "figure"),
            Output("last-update-time", "children")
        ],
        [
            Input("refresh-button", "n_clicks"),
            Input("refresh-interval", "n_intervals"),
            Input("asset-selector", "value"),
            Input("market-type-selector", "value"),
            Input("order-type-selector", "value")
        ]
    )
    def update_market_overview(n_clicks, n_intervals, asset, market_type, order_type):
        # Get latest prices
        latest_prices = price_repo.get_latest_prices(
            asset_symbol=asset,
            market_type=None if market_type == "ALL" else market_type
        )
        
        if not latest_prices:
            return html.Div("No data available"), {}, "No data"
        
        # Filter by order type if needed
        if order_type != "ALL":
            latest_prices = [p for p in latest_prices if p.order_type == order_type]
        
        # Get last update time
        last_update = latest_prices[0].snapshot.timestamp if latest_prices else datetime.now()
        
        # Create price cards
        price_cards = []
        for exchange_name in set(p.exchange.name for p in latest_prices):
            exchange_prices = [p for p in latest_prices if p.exchange.name == exchange_name]
            
            # Calculate min/max/avg prices
            if exchange_prices:
                min_price = min(p.price for p in exchange_prices)
                max_price = max(p.price for p in exchange_prices)
                avg_price = sum(p.price for p in exchange_prices) / len(exchange_prices)
                
                card = dbc.Card([
                    dbc.CardBody([
                        html.H5(exchange_name, className="card-title"),
                        html.P([
                            html.Span("Avg: ", className="fw-bold"),
                            f"${avg_price:.2f}",
                            html.Br(),
                            html.Span("Min: ", className="fw-bold"),
                            f"${min_price:.2f}",
                            html.Br(),
                            html.Span("Max: ", className="fw-bold"),
                            f"${max_price:.2f}"
                        ])
                    ])
                ], className="text-center")
                
                price_cards.append(dbc.Col(card, width=3, className="mb-3"))
        
        # Create price comparison chart
        df = pd.DataFrame([
            {
                "Exchange": p.exchange.name,
                "Price": p.price,
                "OrderType": p.order_type,
                "MarketType": p.market_type
            }
            for p in latest_prices
        ])
        
        if df.empty:
            fig = {}
        else:
            fig = px.box(
                df,
                x="Exchange",
                y="Price",
                color="OrderType",
                title=f"{asset} Price Comparison",
                points="all"
            )
            
            fig.update_layout(
                yaxis_title="Price (USD)",
                xaxis_title="Exchange",
                legend_title="Order Type",
                height=500
            )
        
        # Format last update time
        update_text = f"Last updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')}"
        
        return dbc.Row(price_cards), fig, update_text
    
    @app.callback(
        Output("opportunities-table", "children"),
        [
            Input("refresh-button", "n_clicks"),
            Input("refresh-interval", "n_intervals"),
            Input("asset-selector", "value"),
            Input("min-profit-slider", "value")
        ]
    )
    def update_opportunities(n_clicks, n_intervals, asset, min_profit):
        # Find opportunities
        opportunities = opportunity_finder.find_opportunities(
            asset=asset,
            min_profit_percentage=min_profit
        )
        
        if not opportunities:
            return html.Div("No arbitrage opportunities found", className="text-center p-3")
        
        # Create opportunities table
        table_header = [
            html.Thead(html.Tr([
                html.Th("Buy From"),
                html.Th("Sell To"),
                html.Th("Buy Price"),
                html.Th("Sell Price"),
                html.Th("Available Amount"),
                html.Th("Transfer Fee"),
                html.Th("Profit"),
                html.Th("Profit %"),
                html.Th("Details")
            ]))
        ]
        
        rows = []
        for opp in opportunities:
            row = html.Tr([
                html.Td(opp.buy_exchange),
                html.Td(opp.sell_exchange),
                html.Td(f"${opp.buy_price:.2f}"),
                html.Td(f"${opp.sell_price:.2f}"),
                html.Td(f"{opp.available_amount:.2f} {opp.asset}"),
                html.Td(f"${opp.transfer_fee:.2f}"),
                html.Td(f"${opp.potential_profit:.2f}"),
                html.Td(f"{opp.profit_percentage:.2f}%"),
                html.Td(dbc.Button("Details", id=f"details-btn-{opp.buy_exchange}-{opp.sell_exchange}", 
                                 size="sm", color="info"))
            ])
            rows.append(row)
        
        table_body = [html.Tbody(rows)]
        
        return dbc.Table(
            table_header + table_body,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True
        )
    
    @app.callback(
        Output("liquidity-trend-chart", "figure"),
        [
            Input("refresh-button", "n_clicks"),
            Input("refresh-interval", "n_intervals"),
            Input("asset-selector", "value"),
            Input("liquidity-exchange-selector", "value"),
            Input("liquidity-timerange-selector", "value")
        ]
    )
    def update_liquidity_chart(n_clicks, n_intervals, asset, exchange, time_range):
        # Analyze liquidity
        liquidity_data = liquidity_analyzer.analyze_liquidity(
            asset=asset,
            exchange=exchange,
            time_range=time_range,
            interval=1
        )
        
        if not liquidity_data or not liquidity_data["intervals"]:
            return {}
        
        # Create figure
        fig = go.Figure()
        
        # Add buy liquidity trace
        fig.add_trace(go.Scatter(
            x=liquidity_data["intervals"],
            y=liquidity_data["buy_liquidity"],
            mode="lines+markers",
            name="Buy Liquidity",
            line=dict(color="green", width=2),
            marker=dict(size=8)
        ))
        
        # Add sell liquidity trace
        fig.add_trace(go.Scatter(
            x=liquidity_data["intervals"],
            y=liquidity_data["sell_liquidity"],
            mode="lines+markers",
            name="Sell Liquidity",
            line=dict(color="red", width=2),
            marker=dict(size=8)
        ))
        
        # Add total liquidity trace
        fig.add_trace(go.Scatter(
            x=liquidity_data["intervals"],
            y=liquidity_data["total_liquidity"],
            mode="lines+markers",
            name="Total Liquidity",
            line=dict(color="blue", width=2),
            marker=dict(size=8)
        ))
        
        # Update layout
        fig.update_layout(
            title=f"{asset} Liquidity on {exchange} (Last {time_range} hours)",
            xaxis_title="Time",
            yaxis_title=f"Liquidity ({asset})",
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
        )
        
        return fig
    
    return layout