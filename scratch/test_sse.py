import asyncio
import httpx

async def main():
    # Prompt for Scenario B (Stock price retrieval + analysis)
    prompt = "Retrieve stock prices for AAPL, GOOGL, MSFT and analyze trends"
    url = f"http://localhost:8000/api/orchestrate?prompt={prompt}"
    
    print(f"Connecting to {url}...")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            async with client.stream("GET", url) as response:
                print(f"Response status: {response.status_code}")
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data = line[6:]
                        print(f"CHUNK: {data}")
                    elif line.strip() == ": ping":
                        print("PING")
                    elif line.strip():
                        print(f"LINE: {line}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
