#!/usr/bin/env python3
"""
Fundamentals Service - yfinance Integration
Fetch key fundamental metrics and financial ratios using yfinance
Provides comprehensive fundamental analysis data including valuation ratios, profitability metrics, and financial health indicators
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logging.warning("yfinance not installed. Fundamentals service will not be available.")


class FinanceFundamentalsService:
    """Fundamentals Service - Fetch key fundamental metrics using yfinance"""

    def __init__(self):
        self.name = "Fundamentals Service"
        self.version = "1.0.0"
        
        if not YFINANCE_AVAILABLE:
            logging.error("yfinance library is not available. Please install with: pip install yfinance")
        else:
            logging.info(f"Fundamentals Service v{self.version} initialized with yfinance integration")

    async def get_key_metrics(
        self,
        symbol: str,
        user_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch key fundamental metrics and financial ratios using yfinance.
        
        This tool retrieves comprehensive fundamental analysis data including valuation ratios,
        profitability metrics, financial health indicators, and other key metrics for investment analysis.
        
        Args:
            symbol (str): Stock ticker symbol (e.g., "AAPL", "TSLA", "MSFT").
                         This is a required parameter. Case-insensitive.
            
            user_info (Optional[Dict[str, Any]]): User information for logging and tracking purposes.
                                                 Not used for authentication in this service.
        
        Returns:
            Dict[str, Any]: A dictionary containing:
                - success (bool): Whether the operation succeeded
                - data (Dict): Dictionary of fundamental metrics including:
                    * Valuation Metrics:
                        - market_cap: Market capitalization
                        - trailing_pe: Trailing P/E ratio
                        - forward_pe: Forward P/E ratio
                        - price_to_book: Price-to-Book ratio
                        - price_to_sales: Price-to-Sales ratio
                        - peg_ratio: PEG ratio (P/E to Growth)
                        - enterprise_value: Enterprise value
                        - ev_to_revenue: EV/Revenue ratio
                        - ev_to_ebitda: EV/EBITDA ratio
                    * Profitability Metrics:
                        - profit_margins: Net profit margin
                        - operating_margins: Operating margin
                        - gross_margins: Gross margin
                        - return_on_equity: ROE (Return on Equity)
                        - return_on_assets: ROA (Return on Assets)
                        - ebitda: EBITDA
                    * Per-Share Metrics:
                        - trailing_eps: Trailing Earnings Per Share
                        - forward_eps: Forward Earnings Per Share
                        - book_value: Book value per share
                        - revenue_per_share: Revenue per share
                    * Dividend Metrics:
                        - dividend_yield: Dividend yield (as decimal)
                        - dividend_rate: Annual dividend rate
                        - payout_ratio: Dividend payout ratio
                    * Financial Health:
                        - debt_to_equity: Debt-to-Equity ratio
                        - current_ratio: Current ratio
                        - quick_ratio: Quick ratio (Acid-test ratio)
                        - total_cash: Total cash
                        - total_debt: Total debt
                        - free_cashflow: Free cash flow
                    * Growth & Other:
                        - revenue_growth: Revenue growth rate
                        - earnings_growth: Earnings growth rate
                        - beta: Beta (volatility measure)
                        - shares_outstanding: Number of shares outstanding
                        - float_shares: Float shares
                        - held_percent_insiders: Insider ownership percentage
                        - held_percent_institutions: Institutional ownership percentage
                - metadata (Dict): Metadata about the response:
                    * symbol: Stock symbol queried
                    * query_time: Timestamp of the query
                    * data_source: Data provider (yfinance)
                    * currency: Currency of financial data
                - error (Optional[str]): Error message if the operation failed
        
        Raises:
            ValueError: If required parameters are missing or invalid
            Exception: If yfinance API call fails or data processing encounters an error
        
        Examples:
            # Get fundamental metrics for Apple stock
            result = await service.get_key_metrics("AAPL")
            
            # Access specific metrics
            if result['success']:
                pe_ratio = result['data']['trailing_pe']
                market_cap = result['data']['market_cap']
                roe = result['data']['return_on_equity']
        """
        try:
            # Check if yfinance is available
            if not YFINANCE_AVAILABLE:
                return {
                    "success": False,
                    "error": "yfinance library is not installed. Please install with: pip install yfinance",
                    "data": {},
                    "metadata": {}
                }
            
            # Validate required parameters
            if not symbol or not symbol.strip():
                raise ValueError("Stock symbol is required and cannot be empty")
            
            symbol = symbol.strip().upper()
            
            # Log request information
            user_id = user_info.get("user_id") if user_info else "anonymous"
            logging.info(
                f"Fundamentals request - User: {user_id}, Symbol: {symbol}"
            )
            
            # Call yfinance API
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
            except Exception as api_error:
                logging.error(f"yfinance API call failed: {str(api_error)}")
                raise ValueError(f"Failed to fetch data from yfinance: {str(api_error)}")
            
            if not info or len(info) == 0:
                logging.warning(f"No data returned for symbol: {symbol}")
                return {
                    "success": True,
                    "data": {},
                    "metadata": {
                        "symbol": symbol,
                        "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "data_source": "yfinance",
                        "message": "No data available for the specified symbol"
                    }
                }
            
            # Helper function to safely extract numeric values
            def get_numeric(key: str, default=None) -> Optional[float]:
                value = info.get(key)
                if value is None or value == 'null' or value == '':
                    return default
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return default
            
            # Helper function to safely extract string values
            def get_string(key: str, default=None) -> Optional[str]:
                value = info.get(key)
                return value if value and value != 'null' else default
            
            # Extract fundamental metrics
            metrics = {
                # Valuation Metrics
                "market_cap": get_numeric("marketCap"),
                "trailing_pe": get_numeric("trailingPE"),
                "forward_pe": get_numeric("forwardPE"),
                "price_to_book": get_numeric("priceToBook"),
                "price_to_sales": get_numeric("priceToSalesTrailing12Months"),
                "peg_ratio": get_numeric("pegRatio"),
                "enterprise_value": get_numeric("enterpriseValue"),
                "ev_to_revenue": get_numeric("enterpriseToRevenue"),
                "ev_to_ebitda": get_numeric("enterpriseToEbitda"),
                
                # Profitability Metrics
                "profit_margins": get_numeric("profitMargins"),
                "operating_margins": get_numeric("operatingMargins"),
                "gross_margins": get_numeric("grossMargins"),
                "return_on_equity": get_numeric("returnOnEquity"),
                "return_on_assets": get_numeric("returnOnAssets"),
                "ebitda": get_numeric("ebitda"),
                
                # Per-Share Metrics
                "trailing_eps": get_numeric("trailingEps"),
                "forward_eps": get_numeric("forwardEps"),
                "book_value": get_numeric("bookValue"),
                "revenue_per_share": get_numeric("revenuePerShare"),
                
                # Dividend Metrics
                "dividend_yield": get_numeric("dividendYield"),
                "dividend_rate": get_numeric("dividendRate"),
                "payout_ratio": get_numeric("payoutRatio"),
                
                # Financial Health
                "debt_to_equity": get_numeric("debtToEquity"),
                "current_ratio": get_numeric("currentRatio"),
                "quick_ratio": get_numeric("quickRatio"),
                "total_cash": get_numeric("totalCash"),
                "total_debt": get_numeric("totalDebt"),
                "free_cashflow": get_numeric("freeCashflow"),
                
                # Growth & Other
                "revenue_growth": get_numeric("revenueGrowth"),
                "earnings_growth": get_numeric("earningsGrowth"),
                "beta": get_numeric("beta"),
                "shares_outstanding": get_numeric("sharesOutstanding"),
                "float_shares": get_numeric("floatShares"),
                "held_percent_insiders": get_numeric("heldPercentInsiders"),
                "held_percent_institutions": get_numeric("heldPercentInstitutions"),
            }
            
            # Extract metadata
            metadata = {
                "symbol": symbol,
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_source": "yfinance",
                "currency": get_string("currency", "USD"),
                "company_name": get_string("longName"),
                "sector": get_string("sector"),
                "industry": get_string("industry"),
            }
            
            # Count non-null metrics
            non_null_count = sum(1 for v in metrics.values() if v is not None)
            
            logging.info(
                f"Successfully fetched {non_null_count} fundamental metrics for {symbol}"
            )
            
            return {
                "success": True,
                "data": metrics,
                "metadata": metadata
            }
            
        except ValueError as ve:
            # Handle validation errors
            logging.error(f"Validation error: {str(ve)}")
            return {
                "success": False,
                "error": str(ve),
                "data": {},
                "metadata": {
                    "symbol": symbol if 'symbol' in locals() else None,
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
        except Exception as e:
            # Handle unexpected errors
            logging.error(f"Unexpected error in get_key_metrics: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": f"An unexpected error occurred: {str(e)}",
                "data": {},
                "metadata": {
                    "symbol": symbol if 'symbol' in locals() else None,
                    "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }