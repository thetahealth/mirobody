import logging

from ..utils import execute_query

#-----------------------------------------------------------------------------

async def get_reference_task_detail(reference_task_id: str, user_id: str) -> tuple[str, str]:
    """
    Get today's task details from the database
    
    Args:
        reference_task_id: The task ID to retrieve
    
    Returns:
        A tuple of (task_detail, task_recommend_question)
        - task_detail: String representation of task chunks (excluding recommend questions)
        - task_recommend_question: The recommended question from the task
    """
    try:
        recommend_question_type = 'todayRecommendQuestion'
        
        task_detail = ''
        task_recommend_question = ''
        
        sql = "SELECT * FROM file_parser_tasks WHERE id = :reference_task_id and user_id = :user_id"
        result = await execute_query(
            sql,
            params={"reference_task_id": reference_task_id, "user_id": user_id},
            query_type="select",
            mode="async"
        )
        
        if result and len(result) > 0:
            chunk_list = result[0]["result"]
            new_list = []
            for c in chunk_list:
                chunk_type = c.get("type", '')
                if chunk_type == recommend_question_type:
                    task_recommend_question = c.get("content", '')
                elif chunk_type:
                    new_list.append(dict(type=chunk_type, content=c.get("content", '')))
            task_detail = str(new_list)
            
        return task_detail, task_recommend_question
    except Exception as e:
        logging.error(f"Get today task detail error: {str(e)}", exc_info=True)
        return '', ''

#-----------------------------------------------------------------------------
