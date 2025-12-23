import asyncio
import nats

async def main():
    # Connect to Docker NATS
    nc = await nats.connect("nats://localhost:4222")
    print("AI Agent connected to NATS!")
    
    # Just wait
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())