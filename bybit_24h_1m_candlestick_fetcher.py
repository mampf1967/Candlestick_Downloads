import csv
import time
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
import os
import sys
from typing import List, Dict, Any, Optional

# =================== CONFIGURABLE PARAMETERS ===================
BASE_URL = "https://api.bybit.com"
DATA_FOLDER = "bybit_historical_data"

SESSION_CONFIG = {
    "timeout": 10,
    "connector_limit": 100,
    "ttl_dns_cache": 300
}

TIMEZONE_CONFIG = {
    "vienna_offset": timedelta(hours=2)  # UTC+2 for Vienna
}

# Rate limiting config
RATE_LIMIT_CONFIG = {
    "max_retries": 3,
    "retry_delay": 5,
    "batch_delay": 0.1  # Delay between requests to avoid rate limits
}
# =================== END OF CONFIGURABLE PARAMETERS ===================

session = None

def get_vienna_time(utc_time=None):
    """Convert UTC time to Vienna time"""
    if utc_time is None:
        utc_time = datetime.now(timezone.utc)
    return utc_time.astimezone(timezone(TIMEZONE_CONFIG["vienna_offset"]))

def print_with_timestamp(message: str):
    """Print message with Vienna timestamp"""
    timestamp = get_vienna_time().strftime("%H:%M:%S")
    print(f"{timestamp} {message}")

async def get_session():
    """Get or create aiohttp session"""
    global session
    if session is None:
        timeout = aiohttp.ClientTimeout(total=SESSION_CONFIG["timeout"])
        conn = aiohttp.TCPConnector(
            limit=SESSION_CONFIG["connector_limit"],
            ttl_dns_cache=SESSION_CONFIG["ttl_dns_cache"]
        )
        session = aiohttp.ClientSession(connector=conn, timeout=timeout)
    return session

async def validate_trading_pair(pair: str) -> bool:
    """Validate if the trading pair exists and is active on Bybit Spot"""
    print_with_timestamp(f"Validating trading pair: {pair}")
    session = await get_session()
    
    try:
        async with session.get(f"{BASE_URL}/v5/market/instruments-info?category=spot") as response:
            if response.status == 200:
                data = await response.json()
                if data['retCode'] != 0:
                    print_with_timestamp(f"API error {data['retCode']}: {data['retMsg']}")
                    return False

                valid_pairs = [
                    symbol['symbol'] for symbol in data['result']['list'] 
                    if symbol['status'] == 'Trading' and symbol['symbol'] == pair.upper()
                ]
                
                if valid_pairs:
                    print_with_timestamp(f"OK Trading pair {pair} is valid and active")
                    return True
                else:
                    print_with_timestamp(f"ERROR Trading pair {pair} not found or not active")
                    return False
            else:
                error_text = await response.text()
                print_with_timestamp(f"Failed to validate pair. Status: {response.status}, Message: {error_text}")
                return False
    except Exception as e:
        print_with_timestamp(f"Error validating pair: {e}")
        return False

async def fetch_historical_data(pair: str, start_time: int, end_time: int, limit: int = 1000) -> List[List[Any]]:
    """Fetch historical kline data for a specific time range"""
    session = await get_session()
    retry_count = 0
    max_retries = RATE_LIMIT_CONFIG["max_retries"]
    
    params = {
        'category': 'spot',
        'symbol': pair.upper(),
        'interval': '1',  # 1 minute intervals
        'start': start_time,
        'end': end_time,
        'limit': limit
    }
    
    while retry_count < max_retries:
        try:
            async with session.get(f"{BASE_URL}/v5/market/kline", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data['retCode'] != 0:
                        if data['retCode'] == 10006:  # Rate limit error
                            retry_after = 10
                            print_with_timestamp(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                            await asyncio.sleep(retry_after)
                            retry_count += 1
                            continue
                        else:
                            print_with_timestamp(f"API error {data['retCode']}: {data['retMsg']}")
                            return []

                    # Convert data to expected format
                    converted_data = [
                        [
                            int(candle[0]),       # Open time (timestamp)
                            float(candle[1]),     # Open
                            float(candle[2]),     # High
                            float(candle[3]),     # Low
                            float(candle[4]),     # Close
                            float(candle[5]),     # Volume
                            float(candle[6]),     # Turnover
                        ] 
                        for candle in data['result']['list']
                    ]
                    
                    # Sort by timestamp (ascending)
                    converted_data.sort(key=lambda x: x[0])
                    return converted_data
                    
                elif response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    print_with_timestamp(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    retry_count += 1
                else:
                    error_text = await response.text()
                    print_with_timestamp(f"Failed to fetch data. Status: {response.status}, Message: {error_text}")
                    return []
                    
        except Exception as e:
            print_with_timestamp(f"Error fetching data (attempt {retry_count + 1}/{max_retries}): {e}")
            retry_count += 1
            await asyncio.sleep(RATE_LIMIT_CONFIG["retry_delay"])
    
    print_with_timestamp(f"Failed to fetch data after {max_retries} attempts")
    return []

async def fetch_24h_data(pair: str) -> List[List[Any]]:
    """Fetch complete 24-hour historical data"""
    print_with_timestamp(f"Fetching 24-hour data for {pair}")
    
    # Calculate time range (24 hours ago to now)
    end_time = int(datetime.now(timezone.utc).timestamp() * 1000)
    start_time = end_time - (24 * 60 * 60 * 1000)  # 24 hours in milliseconds
    
    print_with_timestamp(f"Time range: {datetime.fromtimestamp(start_time/1000, tz=timezone.utc)} to {datetime.fromtimestamp(end_time/1000, tz=timezone.utc)} UTC")
    
    all_candles = []
    current_start = start_time
    batch_count = 1
    
    # Bybit returns max 1000 candles per request, so we need to paginate
    while current_start < end_time:
        batch_end = min(current_start + (1000 * 60 * 1000), end_time)  # 1000 minutes in ms
        
        print_with_timestamp(f"Fetching batch {batch_count} (from {datetime.fromtimestamp(current_start/1000, tz=timezone.utc).strftime('%H:%M:%S')} to {datetime.fromtimestamp(batch_end/1000, tz=timezone.utc).strftime('%H:%M:%S')} UTC)")
        
        batch_data = await fetch_historical_data(pair, current_start, batch_end, 1000)
        
        if batch_data:
            all_candles.extend(batch_data)
            print_with_timestamp(f"OK Batch {batch_count}: {len(batch_data)} candles fetched")
        else:
            print_with_timestamp(f"ERROR Batch {batch_count}: Failed to fetch data")
            break
        
        # Update start time for next batch
        if batch_data:
            # Start from the timestamp after the last candle we got
            last_timestamp = batch_data[-1][0]
            current_start = last_timestamp + 60000  # Add 1 minute
        else:
            current_start = batch_end
        
        batch_count += 1
        
        # Rate limiting delay
        await asyncio.sleep(RATE_LIMIT_CONFIG["batch_delay"])
    
    # Remove duplicates and sort
    unique_candles = {}
    for candle in all_candles:
        timestamp = candle[0]
        unique_candles[timestamp] = candle
    
    final_candles = list(unique_candles.values())
    final_candles.sort(key=lambda x: x[0])
    
    print_with_timestamp(f"Total unique candles collected: {len(final_candles)}")
    return final_candles

def save_to_csv(pair: str, candles: List[List[Any]]) -> str:
    """Save candle data to CSV file"""
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    
    # Generate filename with current Vienna time
    vienna_time = get_vienna_time()
    filename = f"{pair.upper()}_24h_{vienna_time.strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(DATA_FOLDER, filename)
    
    try:
        with open(filepath, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Write header
            writer.writerow([
                'timestamp_ms', 'open_time_utc', 'open_time_vienna', 
                'open', 'high', 'low', 'close', 'volume', 'turnover'
            ])
            
            # Write data
            for candle in candles:
                utc_time = datetime.fromtimestamp(candle[0]/1000, tz=timezone.utc)
                vienna_time = get_vienna_time(utc_time)
                
                writer.writerow([
                    candle[0],  # timestamp_ms
                    utc_time.strftime('%Y-%m-%d %H:%M:%S'),
                    vienna_time.strftime('%Y-%m-%d %H:%M:%S'),
                    candle[1],  # open
                    candle[2],  # high
                    candle[3],  # low
                    candle[4],  # close
                    candle[5],  # volume
                    candle[6]   # turnover
                ])
        
        print_with_timestamp(f"OK Data saved to: {filepath}")
        return filepath
        
    except Exception as e:
        print_with_timestamp(f"ERROR Error saving CSV file: {e}")
        return ""

async def main():
    """Main function"""
    print_with_timestamp("=== Bybit 24-Hour Historical Data Fetcher ===")
    
    # Get trading pair from user input
    if len(sys.argv) > 1:
        pair = sys.argv[1].strip()
    else:
        pair = input("Enter trading pair (e.g., BTCUSDT): ").strip()
    
    if not pair:
        print_with_timestamp("ERROR No trading pair provided")
        return
    
    try:
        # Validate trading pair
        if not await validate_trading_pair(pair):
            print_with_timestamp("ERROR Invalid trading pair. Exiting.")
            return
        
        # Fetch 24-hour data
        print_with_timestamp(f"Starting 24-hour data collection for {pair.upper()}")
        start_time = time.time()
        
        candles = await fetch_24h_data(pair)
        
        if not candles:
            print_with_timestamp("ERROR No data collected. Exiting.")
            return
        
        # Save to CSV
        filepath = save_to_csv(pair, candles)
        
        if filepath:
            end_time = time.time()
            duration = end_time - start_time
            
            # Display summary
            first_candle_time = datetime.fromtimestamp(candles[0][0]/1000, tz=timezone.utc)
            last_candle_time = datetime.fromtimestamp(candles[-1][0]/1000, tz=timezone.utc)
            
            print_with_timestamp("=== COLLECTION COMPLETE ===")
            print_with_timestamp(f"Trading Pair: {pair.upper()}")
            print_with_timestamp(f"Total Candles: {len(candles)}")
            print_with_timestamp(f"Time Range: {first_candle_time.strftime('%Y-%m-%d %H:%M:%S')} to {last_candle_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print_with_timestamp(f"Duration: {duration:.2f} seconds")
            print_with_timestamp(f"File: {filepath}")
        else:
            print_with_timestamp("ERROR Failed to save data")
            
    except Exception as e:
        print_with_timestamp(f"ERROR Fatal error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if session:
            await session.close()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
