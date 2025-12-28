import asyncio
import json
import os
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from nats.js.api import ConsumerConfig, AckPolicy
from groq import Groq

# Initialize Groq Client (Ensure GROQ_API_KEY or OPENAI_API_KEY is set in your environment)
groq_api_key = os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY")
if not groq_api_key:
    raise RuntimeError("Set GROQ_API_KEY or OPENAI_API_KEY with your Groq key")
groq_client = Groq(api_key=groq_api_key)

async def get_ai_verdict(task_id, error_context="Unknown execution failure"):
    """Calls the LLM to analyze the failure and provide a verdict."""
    prompt = f"""
    You are an AstraSched Reasoning Agent. Analyze the following task failure and decide:
    1. RETRY: If the error is transient (network, timeout, temporary resource issue).
    2. STOP: If the error is a permanent code bug, configuration error, or security violation.

    Task ID: {task_id}
    Error Context: {error_context}

    Return ONLY a JSON object in this format:
    {{
        "task_id": "{task_id}",
        "decision": "RETRY" or "STOP",
        "reason": "Short explanation of your reasoning"
    }}
    """
    
    try:
        response = await asyncio.to_thread(
            groq_client.chat.completions.create,
            model="openai/gpt-oss-120b",
            messages=[
                {"role": "system", "content": "You are a specialized SRE assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=1024,
        )
        return json.loads(response.choices[0].message.content)
    except json.JSONDecodeError as decode_error:
        return {
            "task_id": task_id,
            "decision": "STOP",
            "reason": f"Invalid AI response: {decode_error}",
        }
    except Exception as e:
        print(f"   ❌ AI API Error: {e}")
        # Fallback to safe default if AI fails
        return {
            "task_id": task_id,
            "decision": "STOP",
            "reason": f"AI Agent Error: {str(e)}"
        }

async def main():
    # Connect to NATS
    try:
        nc = await nats.connect("nats://localhost:4222")
        print("🤖 AI Agent connected to NATS!")
    except Exception as e:
        print(f"Error connecting to NATS: {e}")
        return

    js = nc.jetstream()

    # Ensure Stream exists
    try:
        await js.add_stream(name="TASKS", subjects=["tasks.>"])
    except Exception:
        pass 

    durable_name = "ai-agent-v2"
    queue_group = "ai-reasoning-group"
    subject = "tasks.events.failed"

    print(f"👀 Watching for failures on '{subject}' using Real AI...")
    
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode('utf-8'))
            task_id = data.get("task_id")
            error_msg = data.get("error", "Unknown execution failure")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"❌ ERR: Failed to parse message: {e}")
            await msg.ack()
            return
        
        if not task_id or not task_id.strip():
            await msg.ack()
            return

        print(f"\n[ANALYSIS] Received Failed Task: {task_id}")
        print(f"   Error Log: {error_msg}")

        # CALL REAL AI with actual error context
        verdict = await get_ai_verdict(task_id, error_context=error_msg)

        print(f"   Verdict: {verdict['decision']} because {verdict['reason']}")

        try:
            await js.publish(
                "tasks.governance.verdict", 
                json.dumps(verdict).encode()
            )
            print("   Sent verdict to Scheduler.")
            await msg.ack()
        except Exception as e:
            print(f"   ERR: Could not publish/ack: {e}")

    conf = ConsumerConfig(
        durable_name=durable_name,
        deliver_group=queue_group,
        filter_subject=subject,
        ack_policy=AckPolicy.EXPLICIT,
    )

    try:
        await js.subscribe(
            subject, 
            queue_group,
            config=conf,
            cb=message_handler,
            manual_ack=True
        )
    except Exception as e:
        print(f"❌ CRITICAL ERROR subscribing: {e}")
        return

    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())