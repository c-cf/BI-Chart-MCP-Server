import uuid
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class MemoResourceManager:
    def __init__(self):
        self.memory = {}  # Store memo resources by ID

    def create_memo(self, content: str) -> str:
        """
        Create a new memo resource.
        
        Args:
            content: The content of the memo
        
        Returns:
            The ID of the created memo
        """
        memo_id = str(uuid.uuid4())
        self.memory[memo_id] = content
        logger.info(f"Created memo with ID {memo_id}")
        return memo_id

    def get_memo(self, memo_id: str) -> Optional[str]:
        """
        Get the content of a memo by ID.
        
        Args:
            memo_id: The ID of the memo
        
        Returns:
            The content of the memo, or None if not found
        """
        return self.memory.get(memo_id)

    def list_memos(self) -> List[Dict[str, Any]]:
        """
        List all memo resources.
        
        Returns:
            A list of memo resource metadata
        """
        return [{"id": memo_id, "content": content} for memo_id, content in self.memory.items()]

    def delete_memo(self, memo_id: str) -> bool:
        """
        Delete a memo by ID.
        
        Args:
            memo_id: The ID of the memo
        
        Returns:
            True if the memo was deleted, False if not found
        """
        if memo_id in self.memory:
            del self.memory[memo_id]
            logger.info(f"Deleted memo with ID {memo_id}")
            return True
        return False