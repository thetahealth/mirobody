"""
My data service - user data distribution query
"""

from typing import Any, Dict

from .database_services import FileParserDatabaseService

class MyDataService:
    """My data service class"""

    @staticmethod
    async def get_user_data_distribution(user_id: str) -> Dict[str, Any]:
        """
        Get user data distribution

        Args:
            user_id: User ID

        Returns:
            Dictionary containing data distribution information

        Raises:
            Exception: Thrown when query fails
        """
        # Ensure user_id is string type
        user_id = str(user_id)
        
        # Call database service layer method
        return await FileParserDatabaseService.get_user_data_distribution(user_id)


# Convenience function, maintaining compatibility with original routes.py
async def get_data_distribution(user_id: str) -> Dict[str, Any]:
    """
    Get user data distribution - convenience function

    Args:
        user_id: User ID

    Returns:
        Data distribution information
    """
    # Ensure user_id is string type
    user_id = str(user_id)
    # Directly call database service layer method
    return await FileParserDatabaseService.get_user_data_distribution(user_id)
