
import unittest
from unittest.mock import MagicMock, patch, AsyncMock
import os
import json
import asyncio

# Set dummy API key to avoid RuntimeError on import
os.environ["GROQ_API_KEY"] = "dummy_key"

# Now import the module under test
from reasoning.agent import agent

class TestReasoningAgent(unittest.IsolatedAsyncioTestCase):

    @patch("reasoning.agent.agent.groq_client")
    async def test_get_ai_verdict_RETRY(self, mock_groq):
        # Setup mock response for RETRY
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content=json.dumps({
                "task_id": "task-123",
                "decision": "RETRY",
                "reason": "Transient network issue"
            })))
        ]
        mock_groq.chat.completions.create.return_value = mock_response

        task_id = "task-123"
        error_context = "Connection reset by peer"
        
        verdict = await agent.get_ai_verdict(task_id, error_context)
        
        self.assertEqual(verdict["decision"], "RETRY")
        self.assertEqual(verdict["task_id"], "task-123")
        self.assertIn("Transient", verdict["reason"])

    @patch("reasoning.agent.agent.groq_client")
    async def test_get_ai_verdict_STOP(self, mock_groq):
        # Setup mock response for STOP
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content=json.dumps({
                "task_id": "task-456",
                "decision": "STOP",
                "reason": "Syntax error"
            })))
        ]
        mock_groq.chat.completions.create.return_value = mock_response

        task_id = "task-456"
        error_context = "SyntaxError: invalid syntax"
        
        verdict = await agent.get_ai_verdict(task_id, error_context)
        
        self.assertEqual(verdict["decision"], "STOP")
        self.assertEqual(verdict["task_id"], "task-456")

    @patch("reasoning.agent.agent.groq_client")
    async def test_get_ai_verdict_JSON_Error(self, mock_groq):
        # Setup mock response with invalid JSON
        mock_response = MagicMock()
        mock_response.choices = [
            MagicMock(message=MagicMock(content="Invalid JSON response"))
        ]
        mock_groq.chat.completions.create.return_value = mock_response

        task_id = "task-789"
        
        verdict = await agent.get_ai_verdict(task_id, "Some error")
        
        self.assertEqual(verdict["decision"], "STOP")
        self.assertIn("Invalid AI response", verdict["reason"])

    @patch("reasoning.agent.agent.groq_client")
    async def test_get_ai_verdict_Exception(self, mock_groq):
        # Setup mock to raise exception
        mock_groq.chat.completions.create.side_effect = Exception("API Error")

        task_id = "task-999"
        
        verdict = await agent.get_ai_verdict(task_id, "Some error")
        
        self.assertEqual(verdict["decision"], "STOP")
        self.assertIn("AI Agent Error", verdict["reason"])

if __name__ == '__main__':
    unittest.main()
