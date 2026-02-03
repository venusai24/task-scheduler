
import unittest
import asyncio
import json
import os
import subprocess
import time
import uuid
import nats
from nats.js.api import StreamConfig

# This test requires:
# 1. A running NATS server at localhost:4222
# 2. GROQ_API_KEY set in environment

class TestAgentIntegration(unittest.IsolatedAsyncioTestCase):
    
    async def asyncSetUp(self):
        # 1. Connect to NATS
        try:
            self.nc = await nats.connect("nats://localhost:4222")
            self.js = self.nc.jetstream()
            
            # Setup Stream if not exists
            try:
                await self.js.add_stream(name="TASKS", subjects=["tasks.>"])
            except Exception:
                await self.js.update_stream(name="TASKS", subjects=["tasks.>"])
                
        except Exception as e:
            self.skipTest(f"NATS not available: {e}")

        # 2. Verify API KEY
        if not os.environ.get("GROQ_API_KEY"):
            # Try to populate from known env if available (local fallback)
            pass

    async def asyncTearDown(self):
        if hasattr(self, 'agent_process') and self.agent_process:
            self.agent_process.terminate()
            try:
                self.agent_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.agent_process.kill()
        
        if hasattr(self, 'nc'):
            await self.nc.close()

    async def test_full_failure_resolution_loop(self):
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        
        # Ensure we pass the API key if it's in our process
        if "GROQ_API_KEY" not in env and os.environ.get("GROQ_API_KEY"):
             env["GROQ_API_KEY"] = os.environ.get("GROQ_API_KEY")

        print("\n[TEST] Starting AI Agent process...")
        self.agent_process = subprocess.Popen(
            ["reasoning/venv/bin/python3", "reasoning/agent/agent.py"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        time.sleep(3)
        if self.agent_process.poll() is not None:
            stdout, stderr = self.agent_process.communicate()
            self.fail(f"Agent process died immediately!\nSTDOUT: {stdout}\nSTDERR: {stderr}")

        sub = await self.nc.subscribe("tasks.governance.verdict")
        
        task_id = f"integration-test-{uuid.uuid4()}"
        failure_payload = {
            "task_id": task_id,
            "error": "Timeout connecting to database at 10.0.0.5:5432 after 3000ms"
        }
        
        print(f"[TEST] Publishing failure for {task_id}...")
        await self.js.publish("tasks.events.failed", json.dumps(failure_payload).encode())

        print("[TEST] Waiting for AI Verdict...")
        try:
            msg = await sub.next_msg(timeout=20)
            data = json.loads(msg.data.decode())
            
            print(f"[TEST] Received Verdict: {data}")
            
            self.assertEqual(data["task_id"], task_id)
            self.assertIn(data["decision"], ["RETRY", "STOP"])
            self.assertTrue(len(data["reason"]) > 5)
            
        except asyncio.TimeoutError:
            # Kill process and read output
            self.agent_process.terminate()
            stdout, stderr = self.agent_process.communicate()
            self.fail(f"Timed out waiting for Agent verdict.\nAgent Logs:\n{stdout}\n{stderr}")

if __name__ == '__main__':
    unittest.main()
