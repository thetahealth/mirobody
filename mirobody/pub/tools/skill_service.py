#!/usr/bin/env python3
"""
Skill Service
Responsible for managing and providing access to skill documents
Implements progressive skill invocation through metadata listing and detailed document reading
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from mirobody.utils import global_config, execute_query


class SkillService:
    """
    Skill Service for progressive skill invocation
    
    This service implements a two-stage skill discovery and usage pattern:
    1. list_skill: Browse all available skills with metadata (name, summary, use cases)
    2. read_skill_document: Read detailed skill instructions from SKILL.md
    
    Skills are organized in packages with this structure:
    skills/
    ├── skill_name_1/
    │   ├── SKILL.md         # Detailed skill prompt and instructions
    │   └── metadata.json    # Skill metadata (name, summary, when_to_use, etc.)
    └── skill_name_2/
        ├── SKILL.md
        └── metadata.json
    """
    
    def __init__(self):
        """Initialize SkillService by loading all skill metadata from configured folders"""
        self.name = "Skill Service"
        self.version = "1.0.0"
        
        # Storage for all loaded skills
        self.skills: Dict[str, Dict[str, Any]] = {}
        
        # Load skills during initialization
        self._load_all_skills()
        
        logging.info(f"SkillService initialized with {len(self.skills)} skills loaded")
    
    def _load_all_skills(self):
        """Load all skills from configured SKILL_DIRS"""
        try:
            # Get skills folder paths from config
            # Expected format: ["path/to/skills1", "path/to/skills2"]
            skills_folders = global_config().get_dirs("SKILL_DIRS")
            if isinstance(skills_folders, str):
                skills_folders = json.loads(skills_folders)
            
            if not skills_folders:
                logging.warning("SKILL_DIRS not configured or empty, no skills will be loaded")
                return
            
            logging.info(f"Loading skills from {len(skills_folders)} configured folders: {skills_folders}")
            
            # Process each configured skills folder
            for folder_path in skills_folders:
                self._load_skills_from_folder(folder_path)
                
        except Exception as e:
            logging.error(f"Error loading skills: {str(e)}", stack_info=True)
    
    def _load_skills_from_folder(self, folder_path: str):
        """
        Load all skill packages from a specific folder
        
        Args:
            folder_path: Path to the skills folder containing skill packages
        """
        try:
            # Convert to absolute path
            if not os.path.isdir(folder_path):
                original_folder_path = folder_path

                folder_path = os.path.join(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                    folder_path
                )

                if not os.path.isdir(folder_path):
                    logging.warning(f"Skills path is not a directory: neither '{original_folder_path}' nor '{folder_path}'")
                    return

            folder = Path(folder_path)
            
            if not folder.exists():
                logging.warning(f"Skills folder does not exist: {folder_path}")
                return

            # Iterate through each subdirectory (skill package)
            for skill_dir in folder.iterdir():
                if not skill_dir.is_dir():
                    continue
                
                # Each skill package should contain metadata.json and SKILL.md
                metadata_file = skill_dir / "metadata.json"
                skill_doc_file = skill_dir / "SKILL.md"
                
                if not metadata_file.exists():
                    logging.warning(f"Skipping {skill_dir.name}: missing metadata.json")
                    continue
                
                if not skill_doc_file.exists():
                    logging.warning(f"Skipping {skill_dir.name}: missing SKILL.md")
                    continue
                
                # Load and validate metadata
                try:
                    with open(metadata_file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                    
                    # Validate required fields
                    required_fields = ["name", "summary", "when_to_use", "when_not_to_use", "tags"]
                    missing_fields = [field for field in required_fields if field not in metadata]
                    
                    if missing_fields:
                        logging.warning(
                            f"Skipping {skill_dir.name}: metadata.json missing required fields: {missing_fields}"
                        )
                        continue
                    
                    # Store skill with its metadata and document path
                    skill_id = skill_dir.name
                    self.skills[skill_id] = {
                        "id": skill_id,
                        "name": metadata["name"],
                        "summary": metadata["summary"],
                        "when_to_use": metadata["when_to_use"],
                        "when_not_to_use": metadata["when_not_to_use"],
                        "tags": metadata["tags"],
                        "skill_document_path": str(skill_doc_file.resolve())
                    }
                    
                    logging.info(f"Loaded skill: {skill_id} - {metadata['name']}")
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON in {metadata_file}: {str(e)}")
                except Exception as e:
                    logging.error(f"Error loading skill {skill_dir.name}: {str(e)}")
                    
        except Exception as e:
            logging.error(f"Error loading skills from folder {folder_path}: {str(e)}", stack_info=True)
    
    async def list_skill(
        self,
        user_info: Dict[str, Any],
        tags: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List all available skills with their metadata.
        
        This tool provides an overview of all registered skills, allowing agents to discover
        which skills are available and understand their capabilities and use cases.
        
        Each skill includes:
        - name: Display name of the skill
        - summary: Brief description of what the skill does
        - when_to_use: Scenarios where this skill should be used
        - when_not_to_use: Scenarios where this skill should NOT be used
        - tags: Categories/keywords for skill classification
        
        After identifying a suitable skill, use read_skill_document() to get detailed instructions.
        
        Args:
            tags: Optional comma-separated list of tags to filter skills
                  Example: "data_analysis,csv" or "sql"
                  If provided, only skills matching at least one tag will be returned
        
        Returns:
            Dictionary containing:
            - success: Boolean indicating if the query was successful
            - data: Object containing:
                - total_skills: Number of skills returned
                - skills: Array of skill metadata objects
                - filter_info: Information about applied filters (if any)
        
        Examples:
            # List all available skills
            list_skill()
            
            # List only data analysis skills
            list_skill(tags="data_analysis")
            
            # List skills related to SQL or database work
            list_skill(tags="sql,database")
        """
        try:
            # Get all file-based system skills
            all_skills = list(self.skills.values())
            
            # Get user_id from user_info
            user_id = user_info.get("user_id") if user_info else None
            
            # Load user custom skills from database
            if user_id:
                try:
                    query_sql = """
                        SELECT id, name, summary, when_to_use, when_not_to_use, tags
                        FROM theta_ai.th_user_custom_skills
                        WHERE user_id = :user_id AND is_deleted = false
                    """
                    user_skills_rows = await execute_query(query_sql, params={"user_id": user_id})
                    
                    # Convert database rows to skill format
                    for row in user_skills_rows:
                        user_skill = {
                            "id": f"custom-skill-{row['id']}",  # Use unique prefix for user skills
                            "name": row["name"],
                            "summary": row["summary"],
                            "when_to_use": json.loads(row["when_to_use"]),
                            "when_not_to_use": json.loads(row["when_not_to_use"]),
                            "tags": json.loads(row["tags"]),
                            "skill_type": "user",  # Mark as user skill
                            "db_id": row['id']  # Store original DB id for reference
                        }
                        all_skills.append(user_skill)
                    
                    logging.info(f"Loaded {len(user_skills_rows)} user custom skills for user {user_id}")
                except Exception as e:
                    logging.error(f"Error loading user custom skills: {str(e)}")
                    # Continue without user skills if database query fails
            
            # Apply tag filter if provided
            if tags and tags.strip():
                tag_list = [tag.strip().lower() for tag in tags.split(",")]
                filtered_skills = []
                
                for skill in all_skills:
                    skill_tags = [tag.lower() for tag in skill.get("tags", [])]
                    # Include skill if it has at least one matching tag
                    if any(filter_tag in skill_tags for filter_tag in tag_list):
                        filtered_skills.append(skill)
                
                result_skills = filtered_skills
                filter_info = {
                    "filtered": True,
                    "tags_filter": tag_list,
                    "total_before_filter": len(all_skills)
                }
            else:
                result_skills = all_skills
                filter_info = {
                    "filtered": False
                }
            
            # Remove internal path information from response
            display_skills = []
            for skill in result_skills:
                display_skill = {
                    "id": skill["id"],
                    "name": skill["name"],
                    "summary": skill["summary"],
                    "when_to_use": skill["when_to_use"],
                    "when_not_to_use": skill["when_not_to_use"],
                    "tags": skill["tags"]
                }
                # Add skill_type if it's a user skill
                if "skill_type" in skill:
                    display_skill["skill_type"] = skill["skill_type"]
                display_skills.append(display_skill)
            
            logging.info(f"Listing {len(display_skills)} skills (tags filter: {tags or 'none'})")
            
            return {
                "success": True,
                "data": {
                    "total_skills": len(display_skills),
                    "skills": display_skills,
                    "filter_info": filter_info
                }
            }
            
        except Exception as e:
            logging.error(f"Error listing skills: {str(e)}", stack_info=True)
            return {
                "success": False,
                "data": {
                    "total_skills": 0,
                    "skills": []
                },
                "error": f"Failed to list skills: {str(e)}"
            }
    
    async def read_skill_document(
        self,
        user_info: Dict[str, Any],
        skill_id: str
    ) -> Dict[str, Any]:
        """
        Read the detailed skill document (SKILL.md) for a specific skill.
        
        This tool retrieves the complete skill instructions and prompts from the SKILL.md file.
        Use this after identifying a suitable skill via list_skill() to get detailed guidance
        on how to use the skill's capabilities.
        
        The SKILL.md typically contains:
        - Detailed instructions on the skill's purpose
        - Step-by-step usage guidelines
        - Examples and best practices
        - Advanced techniques and tips
        - Error handling guidance
        
        Args:
            skill_id: The ID of the skill to read (from list_skill response)
                     Example: "csv_analysis", "sql_expert"
        
        Returns:
            Dictionary containing:
            - success: Boolean indicating if the query was successful
            - data: Object containing:
                - skill_id: The requested skill ID
                - skill_name: Display name of the skill
                - document: Full content of the SKILL.md file
                - summary: Brief summary of the skill
        
        Examples:
            # Read detailed instructions for CSV analysis skill
            read_skill_document(skill_id="csv_analysis")
            
            # Read SQL expert skill document
            read_skill_document(skill_id="sql_expert")
        """
        try:
            # Validate skill_id
            if not skill_id or not skill_id.strip():
                return {
                    "success": False,
                    "data": {},
                    "error": "skill_id is required"
                }
            
            skill_id = skill_id.strip()
            
            # Check if this is a user custom skill (starts with "custom-skill-")
            if skill_id.startswith("custom-skill-"):
                # Extract numeric ID
                try:
                    numeric_id = int(skill_id.replace("custom-skill-", ""))
                except ValueError:
                    return {
                        "success": False,
                        "data": {},
                        "error": f"Invalid custom skill ID format: {skill_id}"
                    }
                
                # Get user_id from user_info
                user_id = user_info.get("user_id") if user_info else None
                if not user_id:
                    return {
                        "success": False,
                        "data": {},
                        "error": "User authentication required to access custom skills"
                    }
                
                # Load user skill from database
                try:
                    query_sql = """
                        SELECT id, name, summary, when_to_use, when_not_to_use, tags, skill_md
                        FROM theta_ai.th_user_custom_skills
                        WHERE id = :skill_id AND user_id = :user_id AND is_deleted = false
                    """
                    results = await execute_query(
                        query_sql,
                        params={"skill_id": numeric_id, "user_id": user_id}
                    )
                    
                    if not results or len(results) == 0:
                        return {
                            "success": False,
                            "data": {},
                            "error": f"Custom skill '{skill_id}' not found or not accessible"
                        }
                    
                    row = results[0]
                    document_content = row["skill_md"]
                    
                    logging.info(f"Read custom skill document: {skill_id} ({len(document_content)} characters)")
                    
                    return {
                        "success": True,
                        "data": {
                            "skill_id": skill_id,
                            "skill_name": row["name"],
                            "summary": row["summary"],
                            "document": document_content,
                            "metadata": {
                                "when_to_use": json.loads(row["when_to_use"]),
                                "when_not_to_use": json.loads(row["when_not_to_use"]),
                                "tags": json.loads(row["tags"])
                            },
                            "skill_type": "user"
                        }
                    }
                except Exception as e:
                    logging.error(f"Error loading custom skill {skill_id}: {str(e)}")
                    return {
                        "success": False,
                        "data": {},
                        "error": f"Error loading custom skill: {str(e)}"
                    }
            
            # Otherwise, it's a system skill from files
            # Check if skill exists
            if skill_id not in self.skills:
                available_ids = list(self.skills.keys())
                return {
                    "success": False,
                    "data": {},
                    "error": f"Skill '{skill_id}' not found. Available skills: {available_ids}"
                }
            
            skill = self.skills[skill_id]
            document_path = skill["skill_document_path"]
            
            # Read the SKILL.md file
            try:
                with open(document_path, 'r', encoding='utf-8') as f:
                    document_content = f.read()
            except FileNotFoundError:
                return {
                    "success": False,
                    "data": {},
                    "error": f"Skill document file not found: {document_path}"
                }
            except Exception as e:
                return {
                    "success": False,
                    "data": {},
                    "error": f"Error reading skill document: {str(e)}"
                }
            
            logging.info(f"Read skill document for: {skill_id} ({len(document_content)} characters)")
            
            return {
                "success": True,
                "data": {
                    "skill_id": skill_id,
                    "skill_name": skill["name"],
                    "summary": skill["summary"],
                    "document": document_content,
                    "metadata": {
                        "when_to_use": skill["when_to_use"],
                        "when_not_to_use": skill["when_not_to_use"],
                        "tags": skill["tags"]
                    },
                    "skill_type": "system"
                }
            }
            
        except Exception as e:
            logging.error(f"Error reading skill document: {str(e)}", stack_info=True)
            return {
                "success": False,
                "data": {},
                "error": f"Failed to read skill document: {str(e)}"
            }

