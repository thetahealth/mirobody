import asyncio, logging

from ..utils import execute_query
from ..user.sharing import get_sharing_service

#-----------------------------------------------------------------------------

async def query_asker_info(user_id: str, query_user_id: str):
    asker_info_sql = "select name from theta_ai.health_app_user where id = :user_id limit 1"
    service = await get_sharing_service()
    
    asker_info_result, beneficiary_info_result = await asyncio.gather(
        execute_query(asker_info_sql, {"user_id": int(user_id)}),
        service.get_query_users_simple(user_id)
    )
    
    data_owner_name = ""
    if asker_info_result:
        asker_info = asker_info_result[0]
        asker_name = asker_info.get("name")
        for row in beneficiary_info_result:
            if row.get("id") == query_user_id:
                data_owner_name = row.get("name")
                break
        
        return f"""
The current questioner is {asker_name}, who is asking questions on behalf of {data_owner_name}, where {data_owner_name} is their nickname for the person he is helping. All the data we retrieved earlier belongs to {data_owner_name}.
""".strip()
    else:
        return ""

#-----------------------------------------------------------------------------

async def fetch_system_user_profile(user_id: str):
    try:
        medical_history = ""

        last_doc_id = -1

        sql = "select * from theta_ai.health_user_profile_by_system where user_id= :user_id and is_deleted=false order by version desc limit 1"
        result = await execute_query(sql, params=dict(user_id=user_id))

        if result:
            user_profile_by_system = result[0]
            last_doc_id = user_profile_by_system.get("last_execute_doc_id", -1)
            medical_history = user_profile_by_system.get('common_part', '')

            category_user_profiles = []

            return dict(
                category_user_profiles=category_user_profiles,
                medical_history=medical_history,
                last_doc_id=last_doc_id,
            )

        return dict(
            category_user_profiles=[],
            medical_history=medical_history,
            last_doc_id=last_doc_id,
        )
    except Exception as e:
        logging.warning(f"fetch system user profile for user {user_id} failed: {str(e)}")

        return dict(
            category_user_profiles=[],
            medical_history="",
            last_doc_id=-1,
        )

#-----------------------------------------------------------------------------
