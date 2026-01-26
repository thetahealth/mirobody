"""
User Custom Skills Router
Handles CRUD operations for user-defined skills
"""

import json
import logging
import traceback
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from mirobody.utils.utils_auth import verify_token
from mirobody.utils import execute_query


router = APIRouter(prefix="/api/skills")


# ============================================================================
# Request/Response Models
# ============================================================================

class CreateSkillRequest(BaseModel):
    """Request body for creating a new skill"""
    name: str = Field(..., min_length=1, max_length=255, description="Skill name")
    summary: str = Field(..., min_length=1, description="Brief description of the skill")
    when_to_use: List[str] = Field(..., min_items=1, description="Scenarios where this skill should be used")
    when_not_to_use: List[str] = Field(..., min_items=1, description="Scenarios where this skill should NOT be used")
    tags: List[str] = Field(default=[], description="Tags for categorization")
    skill_md: str = Field(..., min_length=1, description="Skill document in Markdown format")


class UpdateSkillRequest(BaseModel):
    """Request body for updating a skill"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    summary: Optional[str] = Field(None, min_length=1)
    when_to_use: Optional[List[str]] = None
    when_not_to_use: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    skill_md: Optional[str] = Field(None, min_length=1)


class SkillResponse(BaseModel):
    """Skill response model"""
    id: int
    user_id: str
    name: str
    summary: str
    when_to_use: List[str]
    when_not_to_use: List[str]
    tags: List[str]
    skill_md: str
    created_at: str
    updated_at: str


# ============================================================================
# CRUD Endpoints
# ============================================================================

@router.post("")
async def create_skill(
    request: CreateSkillRequest,
    user_id: str = Depends(verify_token)
):
    """
    Create a new custom skill for the current user
    
    POST /api/skills
    """
    try:
        # Insert into database
        insert_sql = """
            INSERT INTO th_user_custom_skills 
            (user_id, name, summary, when_to_use, when_not_to_use, tags, skill_md)
            VALUES (:user_id, :name, :summary, :when_to_use, :when_not_to_use, :tags, :skill_md)
            RETURNING id, created_at, updated_at
        """
        
        result = await execute_query(
            insert_sql,
            params={
                "user_id": user_id,
                "name": request.name,
                "summary": request.summary,
                "when_to_use": json.dumps(request.when_to_use,ensure_ascii=False),
                "when_not_to_use": json.dumps(request.when_not_to_use,ensure_ascii=False),
                "tags": json.dumps(request.tags,ensure_ascii=False),
                "skill_md": request.skill_md
            }
        )
        
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create skill")
        
        created = result
        
        logging.info(f"User {user_id} created skill: {request.name} (id={created['id']})")
        
        return JSONResponse(
            content={
                "code": 0,
                "msg": "Skill created successfully",
                "data": {
                    "id": created["id"],
                    "name": request.name,
                    "created_at": str(created["created_at"]),
                    "updated_at": str(created["updated_at"])
                }
            },
            status_code=201
        )
        
    except Exception as e:
        logging.error(f"Error creating skill: {str(e)}\n{traceback.format_exc()}")
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to create skill: {str(e)}"},
            status_code=500
        )


@router.get("")
async def list_skills(
    user_id: str = Depends(verify_token)
):
    """
    List all custom skills for the current user
    
    GET /api/skills
    """
    try:
        query_sql = """
            SELECT id, user_id, name, summary, when_to_use, when_not_to_use, 
                   tags, skill_md, created_at, updated_at
            FROM th_user_custom_skills
            WHERE user_id = :user_id AND is_deleted = false
            ORDER BY updated_at DESC
        """
        
        results = await execute_query(query_sql, params={"user_id": user_id})
        
        # Build response
        skills = []
        for row in results:
            skill = {
                "id": row["id"],
                "user_id": row["user_id"],
                "name": row["name"],
                "summary": row["summary"],
                "when_to_use": json.loads(row["when_to_use"]),
                "when_not_to_use": json.loads(row["when_not_to_use"]),
                "tags": json.loads(row["tags"]),
                "skill_md": row["skill_md"],
                "created_at": str(row["created_at"]),
                "updated_at": str(row["updated_at"])
            }
            skills.append(skill)
        
        logging.info(f"User {user_id} listed {len(skills)} skills")
        
        return JSONResponse(
            content={
                "code": 0,
                "msg": "Success",
                "data": {
                    "total": len(skills),
                    "skills": skills
                }
            }
        )
        
    except Exception as e:
        logging.error(f"Error listing skills: {str(e)}\n{traceback.format_exc()}")
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to list skills: {str(e)}"},
            status_code=500
        )


@router.get("/{skill_id}")
async def get_skill(
    skill_id: int,
    user_id: str = Depends(verify_token)
):
    """
    Get a specific skill by ID
    
    GET /api/skills/{skill_id}
    """
    try:
        query_sql = """
            SELECT id, user_id, name, summary, when_to_use, when_not_to_use, 
                   tags, skill_md, created_at, updated_at
            FROM th_user_custom_skills
            WHERE id = :skill_id AND user_id = :user_id AND is_deleted = false
        """
        
        results = await execute_query(
            query_sql,
            params={"skill_id": skill_id, "user_id": user_id}
        )
        
        if not results or len(results) == 0:
            return JSONResponse(
                content={"code": -1, "msg": "Skill not found"},
                status_code=404
            )
        
        row = results[0]
        skill = {
            "id": row["id"],
            "user_id": row["user_id"],
            "name": row["name"],
            "summary": row["summary"],
            "when_to_use": json.loads(row["when_to_use"]),
            "when_not_to_use": json.loads(row["when_not_to_use"]),
            "tags": json.loads(row["tags"]),
            "skill_md": row["skill_md"],
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"])
        }
        
        logging.info(f"User {user_id} retrieved skill {skill_id}")
        
        return JSONResponse(
            content={
                "code": 0,
                "msg": "Success",
                "data": skill
            }
        )
        
    except Exception as e:
        logging.error(f"Error getting skill {skill_id}: {str(e)}\n{traceback.format_exc()}")
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to get skill: {str(e)}"},
            status_code=500
        )


@router.post("/{skill_id}/update")
async def update_skill(
    skill_id: int,
    request: UpdateSkillRequest,
    user_id: str = Depends(verify_token)
):
    """
    Update a skill
    
    POST /api/skills/{skill_id}/update
    """
    try:
        # Build dynamic UPDATE query based on provided fields
        update_fields = []
        params = {"skill_id": skill_id, "user_id": user_id}
        
        if request.name is not None:
            update_fields.append("name = :name")
            params["name"] = request.name
        
        if request.summary is not None:
            update_fields.append("summary = :summary")
            params["summary"] = request.summary
        
        if request.when_to_use is not None:
            update_fields.append("when_to_use = :when_to_use")
            params["when_to_use"] = json.dumps(request.when_to_use, ensure_ascii=False)
        
        if request.when_not_to_use is not None:
            update_fields.append("when_not_to_use = :when_not_to_use")
            params["when_not_to_use"] = json.dumps(request.when_not_to_use, ensure_ascii=False)
        
        if request.tags is not None:
            update_fields.append("tags = :tags")
            params["tags"] = json.dumps(request.tags, ensure_ascii=False)
        
        if request.skill_md is not None:
            update_fields.append("skill_md = :skill_md")
            params["skill_md"] = request.skill_md
        
        if not update_fields:
            return JSONResponse(
                content={"code": -1, "msg": "No fields to update"},
                status_code=400
            )
        
        # Add updated_at field
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        
        update_sql = f"""
            UPDATE th_user_custom_skills
            SET {', '.join(update_fields)}
            WHERE id = :skill_id AND user_id = :user_id AND is_deleted = false
        """
        
        result = await execute_query(update_sql, params=params)
        
        # Check if any row was updated
        if not result or result.get("record_count", 0) == 0:
            return JSONResponse(
                content={"code": -1, "msg": "Skill not found or not authorized"},
                status_code=404
            )
        
        # Query the updated record to get updated_at
        query_sql = """
            SELECT id, updated_at
            FROM th_user_custom_skills
            WHERE id = :skill_id AND user_id = :user_id
        """
        updated_records = await execute_query(
            query_sql,
            params={"skill_id": skill_id, "user_id": user_id}
        )
        
        if updated_records and len(updated_records) > 0:
            updated = updated_records[0]
        else:
            # Fallback if query fails
            updated = {"id": skill_id, "updated_at": None}
        
        logging.info(f"User {user_id} updated skill {skill_id}")
        
        return JSONResponse(
            content={
                "code": 0,
                "msg": "Skill updated successfully",
                "data": {
                    "id": updated["id"],
                    "updated_at": str(updated["updated_at"]) if updated.get("updated_at") else None
                }
            }
        )
        
    except Exception as e:
        logging.error(f"Error updating skill {skill_id}: {str(e)}\n{traceback.format_exc()}")
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to update skill: {str(e)}"},
            status_code=500
        )


@router.post("/{skill_id}/delete")
async def delete_skill(
    skill_id: int,
    user_id: str = Depends(verify_token)
):
    """
    Delete a skill (soft delete)
    
    POST /api/skills/{skill_id}/delete
    """
    try:
        delete_sql = """
            UPDATE th_user_custom_skills
            SET is_deleted = true, updated_at = CURRENT_TIMESTAMP
            WHERE id = :skill_id AND user_id = :user_id AND is_deleted = false
        """
        
        result = await execute_query(
            delete_sql,
            params={"skill_id": skill_id, "user_id": user_id}
        )
        
        # Check if any row was updated
        if not result or result.get("record_count", 0) == 0:
            return JSONResponse(
                content={"code": -1, "msg": "Skill not found or already deleted"},
                status_code=404
            )
        
        logging.info(f"User {user_id} deleted skill {skill_id}")
        
        return JSONResponse(
            content={
                "code": 0,
                "msg": "Skill deleted successfully",
                "data": {"id": skill_id}
            }
        )
        
    except Exception as e:
        logging.error(f"Error deleting skill {skill_id}: {str(e)}\n{traceback.format_exc()}")
        return JSONResponse(
            content={"code": -1, "msg": f"Failed to delete skill: {str(e)}"},
            status_code=500
        )
