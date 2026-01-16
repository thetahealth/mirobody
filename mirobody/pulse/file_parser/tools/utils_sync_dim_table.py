#!/usr/bin/env python3
"""
Dimension Table Sync Tool
Fetches indicators from theta_ai.th_series_data table, inserts new records if they don't exist in theta_ai.th_series_dim dimension table.
Based on the implementation pattern of utils_update_embeddings.py
"""

import asyncio
import sys
import logging
from datetime import datetime
import time
from pathlib import Path
from typing import Any, Dict, List, Set

from mirobody.pulse.file_parser.tools.indicator_classifier.sort_dim import MedicalIndicatorClassifier
from mirobody.utils import execute_query
from mirobody.utils.utils_embedding import get_default_embedding_service

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))


class DimTableSyncer:
    """Dimension table synchronizer"""

    # Class-level shared embedding service
    _shared_embedding_service = None
    _initialization_lock = None

    def __init__(self):
        """Initialize dimension table syncer"""
        pass

    @classmethod
    async def get_embedding_service(cls):
        """Get shared embedding service (singleton pattern)"""
        if cls._shared_embedding_service is None:
            if cls._initialization_lock is None:
                cls._initialization_lock = asyncio.Lock()

            async with cls._initialization_lock:
                # Double-checked locking pattern
                if cls._shared_embedding_service is None:
                    logging.info("🔧 Initializing shared embedding service...")
                    cls._shared_embedding_service = await get_default_embedding_service()
                    if not cls._shared_embedding_service:
                        raise Exception("Failed to initialize embedding service")
                    logging.info("✅ Shared embedding service initialized successfully")

        return cls._shared_embedding_service

    async def initialize(self):
        """Initialize embedding service"""
        await self.get_embedding_service()

    def categorize_indicator(self, indicator: str) -> str:
        """
        Intelligently categorize indicator, return category name

        Args:
            indicator: Indicator name

        Returns:
            str: Category name
        """
        indicator_lower = indicator.lower()

        # Blood-related indicators
        if any(
            keyword in indicator_lower
            for keyword in [
                "blood",
                "hemoglobin",
                "hematocrit",
                "platelet",
                "white blood cell",
                "red blood cell",
                "neutrophil",
                "lymphocyte",
                "monocyte",
                "glucose",
                "cholesterol",
                "triglyceride",
                "bilirubin",
                "creatinine",
                "uric acid",
                "albumin",
                "protein",
            ]
        ):
            return "Blood Test"

        # Cardiovascular-related indicators
        elif any(
            keyword in indicator_lower
            for keyword in [
                "heart",
                "ecg",
                "arrhythmia",
                "bradycardia",
                "tachycardia",
                "heart_rate",
                "hrv",
                "ventricular",
                "atrial",
                "cardiac",
            ]
        ):
            return "Cardiovascular"

        # Sleep-related indicators
        elif any(
            keyword in indicator_lower
            for keyword in [
                "sleep",
                "rem",
                "langchain",
                "awake",
                "sleep_duration",
                "sleep_start",
                "sleep_end",
            ]
        ):
            return "Sleep"

        # Physical activity-related indicators
        elif any(
            keyword in indicator_lower
            for keyword in [
                "steps",
                "exercise",
                "walking",
                "cycling",
                "vo2",
                "floors",
                "distance",
                "speed",
            ]
        ):
            return "Physical Activity"

        # Liver function-related indicators
        elif any(
            keyword in indicator_lower
            for keyword in [
                "alanine",
                "aspartate",
                "alt",
                "ast",
                "alkaline",
                "gamma",
                "liver",
            ]
        ):
            return "Liver Function"

        # Kidney function-related indicators
        elif any(keyword in indicator_lower for keyword in ["creatinine", "urea", "kidney", "renal"]):
            return "Kidney Function"

        # Glucose metabolism-related indicators
        elif any(keyword in indicator_lower for keyword in ["7days_average_blood_glucose", "time_in_range", "glucose"]):
            return "Glucose Metabolism"

        # Default category
        else:
            return "Other"

    @property
    async def embedding_service(self):
        """Property accessor for embedding service"""
        return await self.get_embedding_service()

    async def get_missing_indicators(
        self,
        user_id: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        limit: int = None,
    ) -> Set[str]:
        """
        Get indicators from th_series_data table that don't exist in th_series_dim dimension table

        Args:
            user_id: User ID, optional
            start_time: Start time, optional
            end_time: End time, optional
            limit: Limit the number of indicators returned, optional

        Returns:
            Set[str]: Set of indicators not in dimension table
        """
        logging.info("🔍 Querying missing indicators...")

        # Build base query
        base_query = """
        SELECT DISTINCT sd.indicator
        FROM theta_ai.th_series_data sd
        LEFT JOIN theta_ai.th_series_dim dim ON sd.indicator = dim.original_indicator
        WHERE dim.original_indicator IS NULL
        """

        params = {}
        conditions = []

        # Add user filter condition
        if user_id:
            conditions.append("sd.user_id = :user_id")
            params["user_id"] = user_id

        # Add time range filter conditions (using correct time field names)
        if start_time:
            conditions.append("sd.start_time >= :start_time")
            params["start_time"] = start_time

        if end_time:
            conditions.append("sd.end_time <= :end_time")
            params["end_time"] = end_time

        # Concatenate conditions
        if conditions:
            base_query += " AND " + " AND ".join(conditions)

        # Add sorting and limit
        base_query += " ORDER BY sd.indicator"
        if limit:
            base_query += f" LIMIT {limit}"

        try:
            results = await execute_query(base_query, params)

            missing_indicators = {row["indicator"] for row in results} if results else set()

            logging.info(f"📋 Found {len(missing_indicators)} missing indicators")
            if len(missing_indicators) > 0:
                logging.info(f"   Examples: {list(missing_indicators)[:5]}")

            return missing_indicators

        except Exception as e:
            logging.error(f"❌ Failed to query missing indicators: {str(e)}")
            return set()

    async def generate_embeddings_for_indicators(
        self, indicators: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, List[float]]]:
        """
        Efficiently generate embeddings for indicator list (with deduplication optimization)

        Args:
            indicators: List of indicator names

        Returns:
            Dict[str, Dict[str, List[float]]]: Indicator to embeddings mapping
                Format: {indicator_name: {"original": [...], "standard": [...], "category": [...]}}
        """
        if not indicators:
            return {}

        logging.info(f"🔄 Generating embeddings for {len(indicators)} indicators (with deduplication)...")

        try:
            # Collect unique texts and mappings
            unique_texts = set()  # Deduplicated text set
            indicator_mappings = {}  # Text mapping for each indicator

            # Step 1: Collect all unique texts
            for indicator_map in indicators:
                indicator = indicator_map.get("input_indicator", "")
                indicator_mappings[indicator] = {
                    "original": None,
                    "standard": None,
                    "category": None,
                }

                # Original text
                original_text = str(indicator).strip()
                if original_text and original_text != "nan":
                    unique_texts.add(original_text)
                    indicator_mappings[indicator]["original"] = original_text

                # Standard text (currently same as original, but handle separately if different)
                standard_text = str(indicator_map.get("indicator_description", "")).strip()
                if standard_text and standard_text != "nan" and standard_text != original_text:
                    unique_texts.add(standard_text)
                    indicator_mappings[indicator]["standard"] = standard_text
                elif original_text:
                    indicator_mappings[indicator]["standard"] = original_text  # Reuse original

                # Category text
                category_text = self.categorize_indicator(indicator_map.get("input_indicator", ""))
                if category_text and category_text != "nan":
                    unique_texts.add(category_text)
                    indicator_mappings[indicator]["category"] = category_text

            unique_texts_list = list(unique_texts)

            if not unique_texts_list:
                logging.info("   ❌ No valid texts to process")
                return {}

            # Batch get embeddings
            try:
                all_embeddings = await (await self.embedding_service).get_texts_embeddings(unique_texts_list)
            except Exception as e:
                logging.error(f"❌ Failed to call embedding service: {str(e)}")
                return {}

            if not all_embeddings or len(all_embeddings) != len(unique_texts_list):
                logging.error(f"❌ Failed to generate embeddings, expected {len(unique_texts_list)}, got {len(all_embeddings) if all_embeddings else 0}")
                return {}

            # Validate embedding validity
            for i, embedding in enumerate(all_embeddings):
                if not embedding or not isinstance(embedding, list) or len(embedding) == 0:
                    logging.error(f"❌ Embedding {i + 1} is invalid or empty")
                    return {}

            logging.info(f"   ✅ Successfully obtained {len(all_embeddings)} valid unique embeddings")

            # Build text to embedding mapping
            text_to_embedding = {}
            for i, text in enumerate(unique_texts_list):
                if i < len(all_embeddings):
                    text_to_embedding[text] = all_embeddings[i]

            # Assemble results for each indicator
            results = {}
            for indicator_map in indicators:
                indicator = indicator_map.get("input_indicator", "")
                mappings = indicator_mappings[indicator]

                original_emb = text_to_embedding.get(mappings["original"])
                standard_emb = (
                    text_to_embedding.get(mappings["standard"])
                    if mappings["standard"] != mappings["original"]
                    else text_to_embedding.get(mappings["original"])
                )
                category_emb = text_to_embedding.get(mappings["category"])

                # Validate each embedding exists and is valid
                if not original_emb or not standard_emb or not category_emb:
                    logging.error(f"❌ Indicator '{indicator}' has missing embeddings: original={bool(original_emb)}, standard={bool(standard_emb)}, category={bool(category_emb)}")
                    return {}

                results[indicator] = {
                    "original": original_emb,
                    "standard": standard_emb,
                    "category": category_emb,
                }

            logging.info(f"✅ Successfully generated complete embeddings for {len(results)} indicators")
            return results

        except Exception as e:
            logging.info(f"❌ Failed to generate embeddings: {str(e)}", level="error")
            return {}

    async def insert_missing_indicators(
        self,
        indicators: Set[str],
        generate_embeddings: bool = True,
        batch_size: int = 50,
    ) -> bool:
        """
        Batch insert missing indicators into dimension table

        Args:
            indicators: Set of indicators to insert
            generate_embeddings: Whether to generate embeddings
            batch_size: Batch processing size

        Returns:
            bool: Whether insertion was successful
        """
        if not indicators:
            logging.info("✅ No indicators to insert")
            return True

        # Generate medical classifications for new indicators (batch processing to avoid >1000 limit)
        classifier = MedicalIndicatorClassifier()
        indicators_list = list(indicators)
        categories = []

        # Batch processing, max 100 indicators per batch
        classification_batch_size = 100
        total_batches = (len(indicators_list) + classification_batch_size - 1) // classification_batch_size

        logging.info(f"📦 Splitting {len(indicators_list)} indicators into {total_batches} batches for classification")

        for i in range(0, len(indicators_list), classification_batch_size):
            batch_indicators = indicators_list[i : i + classification_batch_size]
            batch_num = i // classification_batch_size + 1
            t0 = time.time()
            logging.info(f"🔄 Processing batch {batch_num}/{total_batches} classification: {len(batch_indicators)} indicators")

            try:
                batch_categories = await classifier.classify_indicators_batch(batch_indicators)

                if not batch_categories:
                    logging.info(f"❌ Batch {batch_num} medical classification generation failed", level="error")
                    continue

                if len(batch_categories) != len(batch_indicators):
                    logging.warning(f"⚠️ Batch {batch_num} classification result count mismatch, expected {len(batch_indicators)}, got {len(batch_categories)}")

                categories.extend(batch_categories)
                t1 = time.time()
                logging.info(f"✅ Batch {batch_num} completed, got {len(batch_categories)} classification results, cost: {t1 - t0} seconds")

                # Add short delay between batches to avoid API limits
                if i + classification_batch_size < len(indicators_list):
                    await asyncio.sleep(0.5)

            except Exception as e:
                logging.error(f"❌ Batch {batch_num} medical classification processing failed: {str(e)}")
                continue

        if len(categories) == 0:
            logging.info("❌ No medical classifications generated", level="error")
            return False
    
        if len(categories) != len(indicators_list):
            # Find indicators without classification
            categories_list = [category.get("input_indicator", "") for category in categories]
            missing_indicators = set(indicators_list) - set(categories_list)
            logging.warning(f"⚠️ Medical classification: expected {len(indicators_list)}, got {len(categories)}, some indicators may be unclassified: {missing_indicators}")

        logging.info(f"🔍 Generated {len(categories)} medical classifications")
        logging.info(f"   Examples: {categories[:3]}")

        # Create indicator to classification mapping - match by input_indicator field
        indicator_to_classification = {}

        # Build mapping based on input_indicator field from results
        for category_result in categories:
            input_indicator = category_result.get("input_indicator", "").strip()
            if input_indicator:
                indicator_to_classification[input_indicator] = category_result

        logging.info(f"📊 Successfully built {len(indicator_to_classification)} indicator to classification mappings")
        logging.info(f"   ✅ With classification results: {len(categories)}")
        logging.info(f"   📝 Using default values: {len(indicators_list) - len(categories)}")
        logging.info(f"💾 Starting to insert {len(indicators_list)} indicators into dimension table...")

        # Generate embeddings if needed (optimization: batch generation to avoid processing too many at once)
        embeddings_map = {}
        if generate_embeddings:
            logging.info("🔄 Starting embeddings generation, will terminate insertion on failure")

            # If too many indicators, batch generate embeddings to avoid memory and API limits
            if len(indicators_list) > 200:
                logging.info(f"📦 Large number of indicators ({len(indicators_list)}), using batch embeddings generation")
                embeddings_map = {}

                # Batch generate embeddings
                embedding_batch_size = 100
                for i in range(0, len(indicators_list), embedding_batch_size):
                    batch_indicators = indicators_list[i : i + embedding_batch_size]
                    logging.info(f"   🔄 Generating batch {i // embedding_batch_size + 1} embeddings: {len(batch_indicators)} indicators")

                    batch_embeddings = await self.generate_embeddings_for_indicators(batch_categories)

                    # Check if embedding generation was successful
                    if not batch_embeddings:
                        logging.warning(f"❌ Batch {i // embedding_batch_size + 1} embedding generation failed, skipping insertion")
                        continue

                    # Validate each indicator has complete embeddings
                    for indicator in batch_indicators:
                        if indicator not in batch_embeddings:
                            logging.warning(f"❌ Indicator '{indicator}' embedding generation failed, skipping insertion")
                            continue

                        embeddings = batch_embeddings[indicator]
                        if (
                            not embeddings.get("original")
                            or not embeddings.get("standard")
                            or not embeddings.get("category")
                        ):
                            logging.warning(f"❌ Indicator '{indicator}' embedding incomplete, skipping insertion")
                            continue

                    embeddings_map.update(batch_embeddings)

                    # Avoid API limits
                    if i + embedding_batch_size < len(indicators_list):
                        await asyncio.sleep(1)
            else:
                embeddings_map = await self.generate_embeddings_for_indicators(batch_categories)

                # Check if embedding generation was successful
                if not embeddings_map:
                    logging.info("❌ Embeddings generation failed, terminating insertion", level="warning")
                    return False

                # Validate each indicator has complete embeddings
                for indicator in indicators_list:
                    if indicator not in embeddings_map:
                        logging.warning(f"❌ Indicator '{indicator}' embedding generation failed, skipping insertion")
                        continue

                    embeddings = embeddings_map[indicator]
                    if (
                        not embeddings.get("original")
                        or not embeddings.get("standard")
                        or not embeddings.get("category")
                    ):
                        logging.warning(f"❌ Indicator '{indicator}' embedding incomplete, skipping insertion")
                        continue

            logging.info(f"✅ Embeddings generation successful for all {len(indicators_list)} indicators")

        try:
            success_count = 0

            # Batch processing
            for i in range(0, len(indicators_list), batch_size):
                batch_indicators = indicators_list[i : i + batch_size]

                # Prepare batch insert parameters
                batch_params = []
                for indicator in batch_indicators:
                    embeddings = embeddings_map.get(indicator, {}) if generate_embeddings else {}
                    
                    # Validate embedding completeness if embeddings are required
                    if generate_embeddings:
                        # Check if indicator exists in embeddings_map
                        if indicator not in embeddings_map:
                            logging.warning(f"⚠️ Indicator '{indicator}' embedding generation failed, skipping insertion")
                            continue
                        
                        # Check if all required embeddings exist
                        if (
                            not embeddings.get("original")
                            or not embeddings.get("standard")
                            or not embeddings.get("category")
                        ):
                            logging.warning(f"⚠️ Indicator '{indicator}' has incomplete embeddings, skipping insertion")
                            continue
                    
                    category_name = self.categorize_indicator(indicator)

                    # Get medical classification info
                    medical_classification = indicator_to_classification.get(indicator, {})

                    # Format embeddings as PostgreSQL vector format
                    def format_vector(embedding_list):
                        if not embedding_list:
                            return None
                        # PostgreSQL vector format: [val1,val2,val3,...] (no spaces)
                        return "[" + ",".join(str(x) for x in embedding_list) + "]"

                    params = {
                        "original_indicator": indicator,
                        "standard_indicator": indicator,  # Default: use original name as standard name
                        "category_group": category_name,  # Use intelligent classification result
                        "category": category_name,  # Use intelligent classification result
                        "orig_emb": format_vector(embeddings.get("original")),
                        "stand_emb": format_vector(embeddings.get("standard")),
                        "cat_emb": format_vector(embeddings.get("category")),
                        # Add new medical classification fields
                        "diagnosis_recommended_organ": medical_classification.get("diagnosis_recommended_organ", ""),
                        "diagnosis_recommended_system": medical_classification.get("diagnosis_recommended_system", ""),
                        "diagnosis_recommended_disease": medical_classification.get(
                            "diagnosis_recommended_disease", ""
                        ),
                        "indicator_description": medical_classification.get("indicator_description", ""),
                        "department": medical_classification.get("department_classification", ""),
                        "symptom": medical_classification.get("related_symptoms", ""),
                        "updated_at": datetime.now(),
                    }
                    batch_params.append(params)

                # Execute batch insert
                if generate_embeddings and embeddings_map:
                    # Batch insert with embeddings
                    logging.info(f"🔄 Batch inserting {len(batch_params)} indicators with embeddings")

                    if batch_params:
                        insert_query = """
                        INSERT INTO theta_ai.th_series_dim 
                        (original_indicator, standard_indicator, category_group, category, 
                         original_indicator_embedding, standard_indicator_embedding, category_embedding,
                         diagnosis_recommended_organ, diagnosis_recommended_system, diagnosis_recommended_disease,
                         department, symptom, updated_at)
                        VALUES 
                        (:original_indicator, :indicator_description, :category_group, :category,
                         :orig_emb, :stand_emb, :cat_emb,
                         :diagnosis_recommended_organ, :diagnosis_recommended_system, :diagnosis_recommended_disease,
                         :department, :symptom, :updated_at)
                        ON CONFLICT (original_indicator) 
                        DO NOTHING
                        """

                        await execute_query(insert_query, batch_params)
                    else:
                        logging.warning("⚠️ All indicators in current batch skipped due to incomplete embeddings, no SQL insert needed")
                else:
                    # Batch insert without embeddings
                    logging.info(f"🔄 Batch inserting {len(batch_params)} indicators without embeddings")

                    if batch_params:
                        insert_query = """
                        INSERT INTO theta_ai.th_series_dim 
                        (original_indicator, standard_indicator, category_group, category,
                         diagnosis_recommended_organ, diagnosis_recommended_system, diagnosis_recommended_disease,
                         department, symptom, updated_at)
                        VALUES 
                        (:original_indicator, :indicator_description, :category_group, :category,
                         :diagnosis_recommended_organ, :diagnosis_recommended_system, :diagnosis_recommended_disease,
                         :department, :symptom, :updated_at)
                        ON CONFLICT (original_indicator) 
                        DO NOTHING
                        """

                        await execute_query(insert_query, batch_params)
                    else:
                        logging.warning("⚠️ All indicators in current batch skipped, no SQL insert needed")

                success_count += len(batch_params)

            logging.info(f"✅ Successfully inserted {success_count} indicators into dimension table")
            return True

        except Exception as e:
            logging.error(f"❌ Failed to insert indicators into dimension table: {str(e)}, error type: {type(e).__name__}")
            # Note: Don't output traceback to avoid leaking embedding vector data in logs
            return False

    async def update_missing_medical_classifications(
        self, batch_size: int = 50, limit: int = None, generate_embeddings: bool = True
    ) -> Dict[str, Any]:
        """
        Update records in dimension table with missing medical classification fields

        Check records where diagnosis_recommended_organ field is null,
        use classifier to get medical classification info and update corresponding fields

        Args:
            batch_size: Batch processing size
            limit: Limit the number of records to process

        Returns:
            Dict[str, Any]: Update result statistics
        """
        logging.info("🚀 Starting to update missing medical classification fields in dimension table")

        # 1. Query records that need updating
        query = """
            SELECT id, original_indicator
            FROM theta_ai.th_series_dim
            WHERE diagnosis_recommended_organ IS NULL 
               OR diagnosis_recommended_organ = ''
               OR original_indicator_embedding is NULL
            ORDER BY id desc
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            records = await execute_query(query)

            if not records:
                logging.info("✅ No records need medical classification update")
                return {
                    "success": True,
                    "total_found": 0,
                    "total_updated": 0,
                    "failed": 0,
                }

            logging.info(f"📋 Found {len(records)} records that need medical classification update")

            logging.info(f"🔍 Preparing full pipeline processing for {len(records)} records (classification + embedding + database update)")

            classifier = MedicalIndicatorClassifier()

            classification_batch_size = 50
            max_concurrent = 10
            total_batches = (len(records) + classification_batch_size - 1) // classification_batch_size

            logging.info(f"📦 Splitting {len(records)} records into {total_batches} batches for concurrent full pipeline processing, max concurrency: {max_concurrent}")

            # Create semaphore to control concurrency
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_batch_complete(batch_records: list, batch_num: int):
                async with semaphore:
                    batch_start_time = time.time()
                    batch_indicators = [record["original_indicator"] for record in batch_records]
                    
                    logging.info(f"🔄 Starting batch {batch_num}/{total_batches} full pipeline: {len(batch_indicators)} indicators")
                    
                    try:
                        # Step 1: Classification
                        t0 = time.time()
                        batch_categories = await classifier.classify_indicators_batch(batch_indicators)

                        if not batch_categories:
                            logging.error(f"❌ Batch {batch_num} medical classification generation failed")
                            return {"success": 0, "failed": len(batch_records), "error": "Classification failed"}

                        if len(batch_categories) != len(batch_indicators):
                            logging.warning(f"⚠️ Batch {batch_num} classification result count mismatch, expected {len(batch_indicators)}, got {len(batch_categories)}")

                        t1 = time.time()
                        logging.info(f"   ✅ Batch {batch_num} classification completed, got {len(batch_categories)} results, cost: {t1 - t0:.2f}s")

                        # Step 2: Generate embeddings (if needed)
                        embeddings_map = {}
                        if generate_embeddings:
                            t0 = time.time()
                            embeddings_map = await self.generate_embeddings_for_indicators(batch_categories)
                            
                            if not embeddings_map:
                                logging.error(f"❌ Batch {batch_num} embedding generation failed")
                                return {"success": 0, "failed": len(batch_records), "error": "Embedding generation failed"}
                            
                            t1 = time.time()
                            logging.info(f"   ✅ Batch {batch_num} embedding generation completed, cost: {t1 - t0:.2f}s")

                        # Step 3: Create indicator to classification mapping
                        indicator_to_classification = {}
                        for category_result in batch_categories:
                            input_indicator = category_result.get("input_indicator", "").strip()
                            if input_indicator:
                                indicator_to_classification[input_indicator] = category_result

                        # Step 4: Update database
                        t0 = time.time()
                        batch_params = []
                        for record in batch_records:
                            embeddings = embeddings_map.get(record["original_indicator"], {}) if generate_embeddings else {}
                            indicator = record["original_indicator"]
                            record_id = record["id"]

                            # Get corresponding medical classification info
                            classification = indicator_to_classification.get(indicator, {})

                            # Format embeddings as PostgreSQL vector format
                            def format_vector(embedding_list):
                                if not embedding_list:
                                    return None
                                return "[" + ",".join(str(x) for x in embedding_list) + "]"

                            # Prepare update parameters
                            params = {
                                "record_id": record_id,
                                "diagnosis_recommended_organ": classification.get("diagnosis_recommended_organ", ""),
                                "diagnosis_recommended_system": classification.get("diagnosis_recommended_system", ""),
                                "diagnosis_recommended_disease": classification.get("diagnosis_recommended_disease", ""),
                                "indicator_description": classification.get("indicator_description", ""),
                                "orig_emb": format_vector(embeddings.get("original")),
                                "stand_emb": format_vector(embeddings.get("standard")),
                                "cat_emb": format_vector(embeddings.get("category")),
                                "department": classification.get("department_classification", ""),
                                "symptom": classification.get("related_symptoms", ""),
                                "updated_at": datetime.now(),
                            }
                            batch_params.append(params)

                        # Execute batch update
                        if generate_embeddings and embeddings_map:
                            update_query = """
                                UPDATE theta_ai.th_series_dim 
                                SET 
                                    diagnosis_recommended_organ = :diagnosis_recommended_organ,
                                    diagnosis_recommended_system = :diagnosis_recommended_system,
                                    diagnosis_recommended_disease = :diagnosis_recommended_disease,
                                    standard_indicator = :indicator_description,
                                    standard_indicator_embedding = :stand_emb,
                                    category_embedding = :cat_emb,
                                    original_indicator_embedding = :orig_emb,
                                    department = :department,
                                    symptom = :symptom,
                                    updated_at = :updated_at
                                WHERE id = :record_id
                            """
                        else:
                            update_query = """
                                UPDATE theta_ai.th_series_dim 
                                SET 
                                    diagnosis_recommended_organ = :diagnosis_recommended_organ,
                                    diagnosis_recommended_system = :diagnosis_recommended_system,
                                    diagnosis_recommended_disease = :diagnosis_recommended_disease,
                                    standard_indicator = :indicator_description,
                                    department = :department,
                                    symptom = :symptom,
                                    updated_at = :updated_at
                                WHERE id = :record_id
                            """

                        await execute_query(update_query, batch_params)
                        
                        t1 = time.time()
                        batch_total_time = time.time() - batch_start_time
                        logging.info(f"   ✅ Batch {batch_num} database update completed, cost: {t1 - t0:.2f}s")
                        logging.info(f"🎉 Batch {batch_num} full pipeline processing completed: {len(batch_records)} records, total time: {batch_total_time:.2f}s")
                        
                        return {"success": len(batch_records), "failed": 0}

                    except Exception as e:
                        batch_total_time = time.time() - batch_start_time
                        logging.error(f"❌ Batch {batch_num} full pipeline processing failed: {str(e)}, time: {batch_total_time:.2f}s")
                        return {"success": 0, "failed": len(batch_records), "error": str(e)}

            # Create all batch tasks (based on records not indicators)
            tasks = []
            for i in range(0, len(records), classification_batch_size):
                batch_records = records[i : i + classification_batch_size]
                batch_num = i // classification_batch_size + 1
                tasks.append(process_batch_complete(batch_records, batch_num))

            # Execute all batches concurrently
            logging.info(f"🚀 Starting concurrent execution of {len(tasks)} full pipeline batch tasks")
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Aggregate results
            total_success = 0
            total_failed = 0
            successful_batches = 0
            failed_batches = 0
            
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    logging.error(f"❌ Batch {i+1} task execution exception: {str(result)}")
                    failed_batches += 1
                    # Estimate failed count
                    batch_size_actual = min(classification_batch_size, len(records) - i * classification_batch_size)
                    total_failed += batch_size_actual
                elif not result or result.get("success", 0) == 0:
                    failed_batches += 1
                    total_failed += result.get("failed", 0) if result else classification_batch_size
                else:
                    successful_batches += 1
                    total_success += result.get("success", 0)
                    total_failed += result.get("failed", 0)

            logging.info(f"📊 Concurrent full pipeline processing completed: successful batches {successful_batches}, failed batches {failed_batches}")
            logging.info(f"📊 Record statistics: successfully updated {total_success}, failed {total_failed}")

            # Check if any records were successful
            if total_success == 0:
                logging.info("❌ All batches failed medical classification generation", level="error")
                return {
                    "success": False,
                    "total_found": len(records),
                    "total_updated": 0,
                    "failed": len(records),
                }

            # Full pipeline processing completed, output final statistics
            logging.info("🎉 Full pipeline medical classification field update completed!")
            logging.info(f"  - Total records found: {len(records)}")
            logging.info(f"  - Successfully updated: {total_success}")
            logging.info(f"  - Failed to update: {total_failed}")
            logging.info(f"  - Success rate: {total_success / len(records) * 100:.1f}%")

            return {
                "success": total_failed == 0,
                "total_found": len(records),
                "total_updated": total_success,
                "failed": total_failed,
            }

        except Exception as e:
            logging.error(f"❌ Failed to update medical classification fields: {str(e)}", stack_info=True)
            return {
                "success": False,
                "total_found": 0,
                "total_updated": 0,
                "failed": 0,
                "error": str(e),
            }

    async def sync_indicators_from_series_data(
        self,
        user_id: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        generate_embeddings: bool = True,
        batch_size: int = 50,
        limit: int = None,
    ) -> Dict[str, Any]:
        """
        Sync indicators from th_series_data table to dimension table

        Args:
            user_id: User ID, optional
            start_time: Start time, optional
            end_time: End time, optional
            generate_embeddings: Whether to generate embeddings
            batch_size: Batch processing size
            limit: Limit the number of indicators to process

        Returns:
            Dict[str, Any]: Sync result statistics
        """
        logging.info("🚀 Starting to sync indicators from th_series_data to dimension table")
        logging.info(f"📊 Parameters: user_id={user_id}, time_range={start_time} to {end_time}")
        logging.info(f"🔧 Config: generate_embeddings={generate_embeddings}, batch_size={batch_size}, limit={limit}")

        # 1. Get missing indicators
        missing_indicators = await self.get_missing_indicators(
            user_id=user_id, start_time=start_time, end_time=end_time, limit=limit
        )

        if not missing_indicators:
            logging.info("✅ No indicators need to be synced")
            return {"success": True, "total_found": 0, "total_inserted": 0, "failed": 0}

        # 2. Insert missing indicators
        insert_success = await self.insert_missing_indicators(
            indicators=missing_indicators,
            generate_embeddings=generate_embeddings,
            batch_size=batch_size,
        )

        result = {
            "success": insert_success,
            "total_found": len(missing_indicators),
            "total_inserted": len(missing_indicators) if insert_success else 0,
            "failed": 0 if insert_success else len(missing_indicators),
        }

        if insert_success:
            logging.info(f"🎉 Sync completed! Processed {len(missing_indicators)} indicators")
        else:
            logging.info("❌ Sync failed!")

        return result

    @classmethod
    async def cleanup(cls):
        """Clean up shared resources"""
        if cls._shared_embedding_service:
            logging.info("🧹 Cleaning up embedding service resources...")
            cls._shared_embedding_service = None
            logging.info("✅ Resource cleanup completed")

    @classmethod
    async def create_syncer(cls):
        """Factory method to create initialized syncer"""
        syncer = cls()
        await syncer.initialize()
        return syncer


# Convenience functions
async def sync_indicators_for_user(
    user_id: str,
    start_time: datetime,
    end_time: datetime,
    generate_embeddings: bool = True,
    batch_size: int = 50,
    limit: int = None,
) -> Dict[str, Any]:
    """
    Sync indicators within specified time range for a specific user

    Args:
        user_id: User ID
        start_time: Start time
        end_time: End time
        generate_embeddings: Whether to generate embeddings
        batch_size: Batch processing size
        limit: Limit the number of indicators to process

    Returns:
        Dict[str, Any]: Sync result
    """
    try:
        syncer = await DimTableSyncer.create_syncer()

        result = await syncer.sync_indicators_from_series_data(
            user_id=user_id,
            start_time=start_time,
            end_time=end_time,
            generate_embeddings=generate_embeddings,
            batch_size=batch_size,
            limit=limit,
        )

        return result

    except Exception as e:
        logging.info(f"❌ User {user_id} indicators sync failed: {str(e)}", level="error")
        return {
            "success": False,
            "total_found": 0,
            "total_inserted": 0,
            "failed": 0,
            "error": str(e),
        }
    finally:
        await DimTableSyncer.cleanup()


async def sync_all_missing_indicators(
    generate_embeddings: bool = True, batch_size: int = 50, limit: int = None
) -> Dict[str, Any]:
    """
    Sync all missing indicators (no user or time restrictions)

    Args:
        generate_embeddings: Whether to generate embeddings
        batch_size: Batch processing size
        limit: Limit the number of indicators to process

    Returns:
        Dict[str, Any]: Sync result
    """
    try:
        syncer = await DimTableSyncer.create_syncer()

        result = await syncer.sync_indicators_from_series_data(
            generate_embeddings=generate_embeddings, batch_size=batch_size, limit=limit
        )

        return result

    except Exception as e:
        logging.error(f"❌ Full indicators sync failed: {str(e)}", stack_info=True)
        return {
            "success": False,
            "total_found": 0,
            "total_inserted": 0,
            "failed": 0,
            "error": str(e),
        }
    finally:
        await DimTableSyncer.cleanup()


async def update_medical_classifications(
    batch_size: int = 50, limit: int = None, generate_embeddings: bool = True
) -> Dict[str, Any]:
    """
    Update missing medical classification fields in dimension table

    Args:
        batch_size: Batch processing size
        limit: Limit the number of records to process

    Returns:
        Dict[str, Any]: Update result
    """
    try:
        syncer = await DimTableSyncer.create_syncer()

        result = await syncer.update_missing_medical_classifications(
            batch_size=batch_size, limit=limit, generate_embeddings=generate_embeddings
        )

        return result

    except Exception as e:
        logging.info(f"❌ Medical classification field update failed: {str(e)}", level="error")
        return {
            "success": False,
            "total_found": 0,
            "total_updated": 0,
            "failed": 0,
            "error": str(e),
        }
    finally:
        await DimTableSyncer.cleanup()
