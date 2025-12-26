import asyncio
import json
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
# 1. IMPORT API OBJECTS
from nats.js.api import ConsumerConfig, AckPolicy

async def main():
    # Connect to NATS
    try:
        nc = await nats.connect("nats://localhost:4222")
        print("🤖 AI Agent connected to NATS!")
    except Exception as e:
        print(f"Error connecting to NATS: {e}")
        return

    js = nc.jetstream()

    # ---------------------------------------------------------
    # 0. ENSURE STREAM EXISTS
    # ---------------------------------------------------------
    try:
        await js.add_stream(name="TASKS", subjects=["tasks.>"])
        print("🌊 Stream 'TASKS' verified.")
    except Exception:
        pass # Stream exists

    # ---------------------------------------------------------
    # 1. SUBSCRIBE WITH EXPLICIT CONFIGURATION
    # ---------------------------------------------------------
    # We use a new name to avoid conflicts with the broken v3/v4 consumers
    durable_name = "ai-agent-explicit"
    queue_group = "ai-reasoning-group"
    subject = "tasks.events.failed"

    print(f"👀 Watching for failures on '{subject}'...")
    
    async def message_handler(msg):
        # Safely decode the task ID
        try:
            task_id = msg.data.decode('utf-8')
        except UnicodeDecodeError:
            print(f"❌ ERR: Received non-UTF8 data on {msg.subject}. Check scheduler publisher.")
            await msg.ack()  # Ack it anyway to clear the queue
            return
        
        # Validate task_id is not empty
        if not task_id or not task_id.strip():
            print(f"❌ ERR: Received empty task ID on {msg.subject}")
            await msg.ack()
            return

        print(f"\n[ANALYSIS] Received Failed Task: {task_id}")

        # Simulate Thinking
        await asyncio.sleep(2) 

        # Heuristic Logic
        last_char = task_id[-1]
        decision = "RETRY" 
        reason = "Transient Network Glitch (Simulated)"
        
        # ASCII check: Odd = STOP, Even = RETRY
        if ord(last_char) % 2 != 0:
            decision = "STOP"
            reason = "Segmentation Fault (Code Bug)"

        print(f"   Verdict: {decision} because {reason}")

        # Publish Verdict
        payload = {
            "task_id": task_id,
            "decision": decision,
            "reason": reason
        }
        
        try:
            await js.publish(
                "tasks.governance.verdict", 
                json.dumps(payload).encode()
            )
            print("   Sent verdict to Scheduler.")
            await msg.ack()
        except Exception as e:
            print(f"   ERR: Could not publish/ack: {e}")

    # DEFINE CONFIGURATION
    # This forces the server to set 'deliver_group', preventing the error.
    conf = ConsumerConfig(
        durable_name=durable_name,
        deliver_group=queue_group,
        filter_subject=subject,
        ack_policy=AckPolicy.EXPLICIT,
    )

    try:
        await js.subscribe(
            subject, 
            queue_group,        # Client-side binding
            config=conf,        # Server-side configuration (The Fix)
            cb=message_handler,
            manual_ack=True
        )
    except Exception as e:
        print(f"❌ CRITICAL ERROR subscribing: {e}")
        return

    # Keep running
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())