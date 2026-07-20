from dorking_engine import DorkingEngine
import asyncio

async def test():
    res = await DorkingEngine.fallback_search_async("propeller manufacturers", ["contact", "email"])
    print(res)

if __name__ == "__main__":
    asyncio.run(test())
