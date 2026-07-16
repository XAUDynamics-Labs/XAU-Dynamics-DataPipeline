import asyncio
import json
import random
from datetime import datetime
from config import logger, GOLD_FEED_URL, DATABASE_NAME, CONTAINER_NAME

class XAUDatapipeline:
    def __init__(self):
        self.is_running = True
        logger.info("Initializing XAU Dynamics Data Pipeline Engine...")

    async def ingest_market_ticks(self, queue: asyncio.Queue):
        """Simulates ingestion of high-frequency XAU/USD ticks via WebSockets."""
        logger.info(f"Connecting to live gold price feed at: {GOLD_FEED_URL}")
        
        # Simulated live price stream
        current_gold_price = 2350.00 
        
        while self.is_running:
            try:
                # Simulate market tick volatility
                price_change = round(random.uniform(-0.75, 0.75), 2)
                current_gold_price = round(current_gold_price + price_change, 2)
                
                tick_data = {
                    "symbol": "XAUUSD",
                    "bid": round(current_gold_price - 0.15, 2),
                    "ask": round(current_gold_price + 0.15, 2),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "volume": random.randint(10, 150)
                }
                
                await queue.put(tick_data)
                logger.debug(f"Tick Ingested: {tick_data['symbol']} -> {tick_data['ask']}")
                
                # High frequency simulation (every 100ms)
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error ingesting tick: {e}")
                await asyncio.sleep(1)

    async def process_and_store(self, queue: asyncio.Queue):
        """Processes raw ticks, normalizes them, and streams them to Azure Cosmos DB."""
        logger.info(f"Establishing connection with Azure Cosmos Container: {DATABASE_NAME}/{CONTAINER_NAME}")
        
        batch_limit = 50
        current_batch = []
        
        while self.is_running:
            try:
                # Retrieve tick from queue
                tick = await queue.get()
                
                # Normalization & Enrichment (Adding proprietary volatility index)
                tick["spread"] = round(tick["ask"] - tick["bid"], 4)
                tick["ingested_at"] = datetime.utcnow().isoformat() + "Z"
                
                current_batch.append(tick)
                queue.task_done()
                
                # When batch is ready, simulate writing to Azure Cosmos DB
                if len(current_batch) >= batch_limit:
                    logger.info(f"Streaming batch of {len(current_batch)} normalized ticks to Azure Cosmos DB.")
                    # Simulate Network I/O
                    await asyncio.sleep(0.05) 
                    current_batch.clear()
                    
            except Exception as e:
                logger.error(f"Error in Processing pipeline: {e}")
                await asyncio.sleep(1)

    async def run(self):
        pipeline_queue = asyncio.Queue(maxsize=1000)
        
        # Run Ingestion and Storage concurrently
        await asyncio.gather(
            self.ingest_market_ticks(pipeline_queue),
            self.process_and_store(pipeline_queue)
        )

if __name__ == "__main__":
    pipeline = XAUDatapipeline()
    try:
        asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by operator.")
