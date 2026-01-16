import logging, uuid

from datetime import datetime

from ..utils import execute_query
from ..utils.utils_user import get_query_user_id

#-----------------------------------------------------------------------------

async def create_session(user_id: str, query_user_id: str) -> dict:
    try:
        query_user_validation = await get_query_user_id(user_id=query_user_id, query_user_id=user_id, permission=["chat"])
        if not query_user_validation.get("success"):
            return {
                "code"  : -1,
                "msg"   : query_user_validation.get("error"),
                "data"  : {}
            }
        
        #-------------------------------------------------

        session_id = str(uuid.uuid4())
        created_at = datetime.now()
        
        session_sql = """
            INSERT INTO theta_ai.th_sessions (
                session_id, user_id, query_user_id,summary, created_at, in_use
            )
            VALUES (:session_id, :user_id, :query_user_id, :summary, :created_at, :in_use)
            """
        
        await execute_query(
            session_sql,
            params={
                "session_id"    : session_id,
                "summary"       : "",
                "created_at"    : created_at,
                "user_id"       : user_id,
                "query_user_id" : query_user_id,
                "in_use"        : False,
            }
        )

        return {
            "code"  : 0,
            "msg"   : "ok",
            "data"  : {
                "session_id": session_id,
                "created_at": created_at.isoformat()
            }
        }
        
    except Exception as e:
        logging.error(f"Error creating empty session: {str(e)}", exc_info=True)

        return {
            "code"  : -2,
            "msg"   : str(e),
            "data"  : {},
        }

#-----------------------------------------------------------------------------

async def get_session_summaries(user_id: str) -> list[dict[str, any]]:
    """Get all conversation summaries for user from database, only including sessions with text messages"""
    logging.info(f"get_session_summaries: {user_id}")
    try:
        # Modified SQL to only return sessions that contain text messages
        summary_sql = """
            SELECT DISTINCT ts.session_id, ts.summary, ts.created_at, ts.query_user_id
            FROM theta_ai.th_sessions ts
            INNER JOIN theta_ai.th_messages tm ON ts.session_id = tm.session_id 
                AND ts.user_id = tm.user_id
            WHERE ts.user_id = :user_id 
                AND ts.category IS NULL
                AND tm.message_type = 'text'
            ORDER BY ts.created_at DESC
        """
        result = await execute_query(
            summary_sql,
            params={"user_id": user_id}
        )

        formatted_summaries = []
        for summary in result:
            formatted_summary = {
                "session_id": summary.get("session_id"),
                "timestamp": (
                    summary.get("created_at").isoformat() if summary.get("created_at") else datetime.now().isoformat()
                ),
                "summary": summary.get("summary", ""),
                "query_user_id": summary.get("query_user_id", ""),
            }
            formatted_summaries.append(formatted_summary)

        logging.info(f"Retrieved {len(formatted_summaries)} text conversation summaries for user {user_id}")
        return formatted_summaries
    
    except Exception as e:
        logging.error(f"Error loading conversation summaries: {str(e)}", exc_info=True)
        return []

#-----------------------------------------------------------------------------

async def get_session_summaries_by_person(user_id: str) -> list[dict[str, any]]:
    """Get session summaries by person"""
    logging.info(f"get_session_summaries_by_person: {user_id}")
    try:
        
        summary_sql = """
            SELECT ts.session_id, ts.summary, ts.created_at, ts.query_user_id, tu.name, tu.gender, tu.birth, tu.blood
            FROM theta_ai.th_sessions ts
            INNER JOIN theta_ai.health_app_user tu ON ts.query_user_id::integer = tu.id
            WHERE ts.user_id = :user_id
            ORDER BY created_at DESC
        """
        result = await execute_query(
            summary_sql,
            params={"user_id": user_id}
        )
        
        session_by_person = {
            
        }
        
        nickname_map = {}
        
        for _session in result:
            user_name = _session.get("name", "No name")
            user_gender = "Male" if _session.get("gender") == 1 else "Female" if _session.get("gender") == 2 else "Other"
            user_birth = _session.get("birth", "")
            user_age = ""
            query_user_id = _session.get("query_user_id")

            if user_birth:
                try:
                    user_birth_date = datetime.strptime(user_birth, "%Y-%m-%d")
                    user_age = datetime.now().year - user_birth_date.year
                except Exception as e:
                    logging.warning(f"Error calculating user age: {str(e)}")
                    user_age = ""
            else:
                user_age = ""

            user_blood = _session.get("blood", "")
            
            if query_user_id != user_id:
                if query_user_id not in nickname_map:
                    query_user_nickname_sql = "select nickname from theta_ai.th_share_user_config where setter_user_id = :user_id and target_user_id = :query_user_id limit 1"
                    query_user_nickname_result = await execute_query(
                        query_user_nickname_sql,
                        params={"user_id": user_id, "query_user_id": query_user_id}
                    )
                    if query_user_nickname_result:
                        user_name = query_user_nickname_result[0].get("nickname")
                        nickname_map[query_user_id] = user_name
                else:
                    user_name = nickname_map[query_user_id]
            
            session_by_person.setdefault((user_name, user_gender, user_age, user_blood), []).append(
                dict(
                    session_id=_session.get("session_id"),
                    query_user_id=_session.get("query_user_id"),
                    timestamp=_session.get("created_at").isoformat() if _session.get("created_at") else datetime.now().isoformat(),
                    summary=_session.get("summary", ""),
                )
            )
            
        return dict(
            code=0,
            msg="ok",
            data=[
                dict(
                    person_name=person_name,
                    gender=user_gender,
                    age=user_age,
                    blood=user_blood,
                    sessions=sessions,
                )
                for (person_name, user_gender, user_age, user_blood), sessions in session_by_person.items()
            ]
        )

    except Exception as e:
        logging.error(f"Error loading conversation summaries: {str(e)}", exc_info=True)

        return dict(
            code=1,
            msg=str(e),
            data=[]
        )

#-----------------------------------------------------------------------------

async def delete_session(user_id: str, session_id: str) -> str | None:
    try:
        # First delete all messages in the session
        delete_messages_sql = """
            DELETE FROM theta_ai.th_messages 
            WHERE user_id = :user_id AND session_id = :session_id
        """
        await execute_query(
            delete_messages_sql,
            params={"user_id": user_id, "session_id": session_id}
        )

        # Then delete the session summary
        delete_session_sql = """
            DELETE FROM theta_ai.th_sessions 
            WHERE user_id = :user_id AND session_id = :session_id
        """
        await execute_query(
            delete_session_sql,
            params={"user_id": user_id, "session_id": session_id}
        )

        return None
        
    except Exception as e:
        return str(e)

#-----------------------------------------------------------------------------
