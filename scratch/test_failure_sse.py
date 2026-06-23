import asyncio
import httpx

async def main():
    # 1. Enable failure injection
    async with httpx.AsyncClient(timeout=10.0) as client:
        print("Injecting failure...")
        res = await client.post("http://localhost:8000/api/inject_failure", json={"enabled": True})
        print(f"Inject failure response: {res.json()}")
        
        # 2. Query system status
        res = await client.get("http://localhost:8000/api/system_status")
        print(f"System status: {res.json()}")
        
        # 3. Stream Scenario B
        prompt = "Retrieve stock prices for AAPL, GOOGL, MSFT and analyze trends"
        url = f"http://localhost:8000/api/orchestrate?prompt={prompt}"
        print(f"Connecting to SSE stream under failure: {url}...")
        
        async with client.stream("GET", url) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data = line[6:]
                    print(f"CHUNK: {data}")
                elif line.strip() == ": ping":
                    pass
                    
        # 4. Disable failure injection
        print("Clearing failure...")
        res = await client.post("http://localhost:8000/api/inject_failure", json={"enabled": False})
        print(f"Clear failure response: {res.json()}")
        
        # 5. Query system status again
        res = await client.get("http://localhost:8000/api/system_status")
        print(f"Final System status: {res.json()}")

if __name__ == "__main__":
    asyncio.run(main())
