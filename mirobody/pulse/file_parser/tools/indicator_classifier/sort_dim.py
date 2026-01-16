# -*- coding: utf-8 -*-
"""
Medical Indicator Intelligent Classifier - Simplified Version
Focused on precise classification of single medical indicators
"""

import json
import os
import re
import sys
import traceback
from typing import Dict, List


sys.path.append(".")

from mirobody.pulse.file_parser.tools.indicator_classifier.prompts import (
    MEDICAL_INDICATOR_CLASSIFICATION_SCHEMA,
    MEDICAL_INDICATOR_CLASSIFICATION_SYSTEM_PROMPT,
    MEDICAL_INDICATOR_CLASSIFICATION_USER_PROMPT,
)
from mirobody.utils.llm import async_get_structured_output
import logging


class MedicalIndicatorClassifier:
    """Simplified medical indicator classifier - focused on precise classification of single medical indicators"""

    def __init__(self, knowledge_graph_path: str = None):
        """Initialize classifier"""
        # Diagnosis result cache
        self.diagnosis_cache = {}

        # Auto-detect and load knowledge graph
        self.knowledge_graph = None
        self.indexes = None

        # If no path specified, auto-find knowledge graph file
        if knowledge_graph_path is None:
            # Get current file's directory path
            current_dir = os.path.dirname(os.path.abspath(__file__))

            # Inline auto-detect knowledge graph logic - using absolute paths
            possible_paths = [
                os.path.join(current_dir, "json_data", "disease_department_symptom.json"),
                os.path.join(current_dir, "disease_department_symptom.json"),
            ]

            logging.info("🔍 Auto-detecting knowledge graph file...")
            logging.info(f"🔍 Search path list: {len(possible_paths)} candidate locations")
            for idx, path in enumerate(possible_paths, 1):
                logging.info(f"   {idx}. {path}")
            
            knowledge_graph_path = None
            for path in possible_paths:
                logging.info(f"🔍 Checking path: {path}")
                if os.path.exists(path):
                    file_size = os.path.getsize(path) / (1024 * 1024)  # MB
                    logging.info(f"✅ Found knowledge graph file: {path} ({file_size:.1f}MB)")
                    knowledge_graph_path = path
                    break

            if not knowledge_graph_path:
                logging.warning("📝 Knowledge graph file not found")
                logging.info(f"🔍 Searched paths: {len(possible_paths)} candidate locations")
                for path in possible_paths:
                    logging.info(f"   - {path} {'exists' if os.path.exists(path) else 'not exists'}")
                logging.info(f"🔍 Current working directory: {os.getcwd()}")
                logging.info(f"🔍 Code file directory: {current_dir}")

        if knowledge_graph_path:
            self._load_knowledge_graph(knowledge_graph_path)

        logging.info("🏥 Medical indicator classifier initialization completed")
        if self.knowledge_graph:
            metadata = self.knowledge_graph.get("metadata", {})
            stats = metadata.get("statistics", {})
            logging.info("📊 Knowledge graph loaded:")
            logging.info(f"  - Disease count: {stats.get('total_diseases', len(self.knowledge_graph.get('diseases', [])))}")
            logging.info(f"  - Department count: {len(self.indexes.get('departments', []))}")
            logging.info(f"  - Symptom count: {len(self.indexes.get('symptoms', []))}")
            logging.info(f"  - Knowledge graph file: {knowledge_graph_path}")
        else:
            logging.warning("⚠️  Knowledge graph not loaded, using basic LLM classification only")

    def _load_knowledge_graph(self, file_path: str):
        """Load medical knowledge graph and build indexes"""
        try:
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    self.knowledge_graph = json.load(f)

                # Inline index building logic
                if self.knowledge_graph:
                    logging.info("🔨 Building knowledge graph indexes...")

                    self.indexes = {
                        "departments": set(),
                        "symptoms": set(),
                        "disease_to_departments": {},
                        "disease_to_symptoms": {},
                    }

                    # Iterate through all diseases to build indexes
                    for disease in self.knowledge_graph["diseases"]:
                        disease_name = disease["name"]
                        departments = disease.get("cure_department", [])
                        symptoms = disease.get("symptom", [])

                        # Collect all departments and symptoms
                        self.indexes["departments"].update(departments)
                        self.indexes["symptoms"].update(symptoms)

                        # Build mappings
                        self.indexes["disease_to_departments"][disease_name] = departments
                        self.indexes["disease_to_symptoms"][disease_name] = symptoms

                    logging.info("✅ Index building completed")

                logging.info(f"✅ Knowledge graph loaded successfully: {file_path}")
            else:
                logging.warning(f"📝 Knowledge graph file not found: {file_path}")

        except Exception as e:
            logging.error(f"⚠️  Knowledge graph loading failed: {e}")
            self.knowledge_graph = None
            self.indexes = None

    # ===== 🎯 Core Classification Functions =====

    async def classify_indicators_batch(self, indicators: List[str]) -> List[Dict[str, str]]:
        """
        🎯 Medical indicator batch classification function: intelligently classify multiple medical indicators at once

        Args:
            indicators: List of indicator names, format: ["blood_pressure", "blood_glucose", "heart_rate", ...]

        Returns:
            List[Dict]: List of classification results, each element contains original input and classification result
            [
                {
                    "input_indicator": "blood_pressure",
                    "diagnosis_recommended_organ": "heart",
                    "diagnosis_recommended_system": "cardiovascular_system",
                    "diagnosis_recommended_disease": "hypertension,arrhythmia",
                    "indicator_description": "Heart rate is the number of heartbeats per minute. Fast heart rate may cause palpitations, chest tightness, slow heart rate may cause dizziness, fatigue, long-term abnormal heart rate may indicate heart disease.",
                    "department_classification": "internal_medicine,cardiology",
                    "related_symptoms": "headache,palpitations,chest_tightness"
                },
                ...
            ]
        """
        if not indicators:
            logging.warning("⚠️ Input indicator list is empty")
            return []

        # Filter empty indicators
        valid_indicators = [indicator.strip() for indicator in indicators if indicator.strip()]
        if not valid_indicators:
            logging.warning("⚠️ All indicator names are empty")
            return []

        logging.info("🚀 Starting batch classification processing...")
        logging.info(f"📊 Number of indicators to process: {len(valid_indicators)}")
        logging.info(f"📋 Indicator list: {', '.join(valid_indicators)}")

        # Step 1: LLM batch intelligent diagnosis analysis
        logging.info("🤖 Step 1: LLM batch intelligent diagnosis analysis...")
        llm_results = await self._llm_diagnosis_batch(valid_indicators)

        # logging.info(f"🔍 LLM batch diagnosis analysis result: {json.dumps(llm_results, ensure_ascii=False)}")

        if not llm_results:
            logging.error("LLM batch diagnosis analysis failed")
            return []

        # Initialize result list
        results = []
        for i, indicator in enumerate(valid_indicators):
            llm_result = llm_results.get(indicator, {})
            result = {
                "input_indicator": indicator,
                "diagnosis_recommended_organ": llm_result.get("recommended_organ", ""),
                "diagnosis_recommended_system": llm_result.get("recommended_system", ""),
                "diagnosis_recommended_disease": llm_result.get("recommended_disease", ""),
                "indicator_description": llm_result.get("indicator_description", ""),
                "department_classification": "",
                "related_symptoms": "",
            }
            if (
                not result["diagnosis_recommended_disease"]
                or not result["diagnosis_recommended_organ"]
                or not result["diagnosis_recommended_system"]
                or not result["indicator_description"]
            ):
                continue

            results.append(result)

        logging.info("✅ LLM batch diagnosis completed")

        # Step 2: Knowledge graph parallel enhancement analysis
        if self.knowledge_graph and self.indexes:
            logging.info("\n🕸️ Step 2: Knowledge graph parallel enhancement analysis...")
            await self._enhance_with_knowledge_graph_parallel(results)
            logging.info("✅ Knowledge graph enhancement completed")
        else:
            logging.warning("⚠️ Knowledge graph not loaded, skipping enhancement analysis")

        logging.info("\n✅ Batch processing completed!")
        logging.info(f"  - Total input count: {len(indicators)}")
        logging.info(f"  - Valid processed: {len(results)}")
        logging.info(f"  - Processing rate: {len(results) / len(indicators) * 100:.1f}%")

        return results

    # ===== 🔧 Core Processing Methods =====

    async def _llm_diagnosis_batch(self, indicators: List[str]) -> Dict[str, dict]:
        """LLM batch diagnosis method - supports automatic chunking for large batches"""
        if not indicators:
            return {}

        # Build batch prompt
        indicators_text = "\n".join([f"{i + 1}. {indicator}" for i, indicator in enumerate(indicators)])

        # Use prompt template
        prompt = MEDICAL_INDICATOR_CLASSIFICATION_USER_PROMPT.format(
            indicators_text=indicators_text, indicator_count=len(indicators)
        )

        try:
            logging.info(f"🤖 Sending single batch diagnosis request with {len(indicators)} indicators...")

            # Build messages
            messages = [
                {
                    "role": "system",
                    "content": MEDICAL_INDICATOR_CLASSIFICATION_SYSTEM_PROMPT,
                },
                {"role": "user", "content": prompt + "\n\nPlease return the analysis result in JSON format."},
            ]
            
            # Use unified LLM interface (auto-selects provider based on available API keys)
            content = await async_get_structured_output(
                messages=messages,
                response_format=MEDICAL_INDICATOR_CLASSIFICATION_SCHEMA,
                temperature=0.1,
                
            )
            if not content:
                logging.error("LLM returned empty content")
                return {}

            logging.info(f"🔍 LLM single batch diagnosis result: {json.dumps(content, ensure_ascii=False)[:500]}...")

            batch_result = self._parse_structured_output_result(content, indicators)

            logging.info(f"✅ Single batch diagnosis completed, successfully parsed {len(batch_result)} results")
            return batch_result

        except Exception:
            logging.error(f"⚠️ LLM single batch diagnosis API call failed: {traceback.format_exc()}", stack_info=True)
            return {}

    def _parse_structured_output_result(self, content: dict, indicators: List[str]) -> Dict[str, dict]:
        """Parse structured output result"""
        logging.info(f"🔍 Starting to parse structured output result, expecting {len(indicators)} indicators...")

        try:
            results = {}
            if "results" in content and isinstance(content["results"], list):
                parsed_count = 0
                for item in content["results"]:
                    indicator_name = item.get("indicator", "").strip()
                    if indicator_name:
                        # Check if all required fields have values
                        recommended_organ = item.get("recommended_organ", "").strip()
                        recommended_system = item.get("recommended_system", "").strip()
                        recommended_disease = item.get("recommended_disease", "").strip()
                        indicator_description = item.get("indicator_description", "").strip()

                        # Only add to results if all three fields have values
                        if recommended_organ and recommended_system and recommended_disease:
                            results[indicator_name] = {
                                "recommended_organ": recommended_organ,
                                "recommended_system": recommended_system,
                                "recommended_disease": recommended_disease,
                                "indicator_description": indicator_description,
                            }
                            parsed_count += 1

                logging.info(f"✅ Successfully parsed {parsed_count} indicator results")

                # Display parsed indicator names
                parsed_indicators = list(results.keys())
                logging.info(f"🔍 Parsed indicators: {', '.join(parsed_indicators[:10])}{'...' if len(parsed_indicators) > 10 else ''}")

                # Check for missing indicators
                missing_indicators = [ind for ind in indicators if ind not in results]
                if missing_indicators:
                    logging.warning(f"⚠️ Missing results for {len(missing_indicators)} indicators:")
                    logging.info(f"   Missing indicators: {', '.join(missing_indicators[:5])}{'...' if len(missing_indicators) > 5 else ''}")

            else:
                logging.error("❌ Structured output format incorrect, missing 'results' field or wrong format")
                results = {}

            logging.info(f"🎉 Finally returning {len(results)} results")
            return results

        except Exception:
            logging.error(f"⚠️ Structured output result parsing failed: {traceback.format_exc()}", stack_info=True)
            return {}

    async def _enhance_with_knowledge_graph_parallel(self, results: List[Dict[str, str]]) -> None:
        """Knowledge graph parallel enhancement analysis"""
        if not self.knowledge_graph or not self.indexes:
            logging.warning("⚠️ Knowledge graph not loaded, skipping enhancement analysis")
            return

        logging.info(f"🔍 Starting parallel enhancement for {len(results)} indicators...")

        for i, result in enumerate(results):
            try:
                diseases = result.get("diagnosis_recommended_disease", "")
                if not diseases:
                    continue

                # Split disease string
                disease_list = [d.strip() for d in re.split(r"[,，、；;|]", diseases) if d.strip()]

                departments = set()
                symptoms = set()

                # Find departments and symptoms for each disease
                for disease in disease_list:
                    if disease in self.indexes["disease_to_departments"]:
                        departments.update(self.indexes["disease_to_departments"][disease])

                    if disease in self.indexes["disease_to_symptoms"]:
                        symptoms.update(self.indexes["disease_to_symptoms"][disease])

                # Update results
                result["department_classification"] = ", ".join(list(departments)[:5])  # Max 5 departments
                result["related_symptoms"] = ", ".join(list(symptoms)[:15])  # Max 15 symptoms

                # Display progress
                if (i + 1) % 10 == 0 or i == len(results) - 1:
                    logging.info(f"📈 Enhancement progress: {i + 1}/{len(results)}")

            except Exception as e:
                logging.error(f"⚠️ Knowledge graph enhancement failed for indicator {i + 1}: {e}")
                result["department_classification"] = "Enhancement failed"
                result["related_symptoms"] = "Enhancement failed"

        logging.info("✅ Parallel enhancement analysis completed")


# 🎯 Medical Indicator Classification Usage Example
if __name__ == "__main__":
    import asyncio

    async def main():
        logging.info("🏥 Medical Indicator Intelligent Classifier - Simplified Version")
        logging.info("=" * 50)

        # Create classifier
        classifier = MedicalIndicatorClassifier()

        logging.info("\n📋 Usage example:")

        try:
            # Prepare batch data
            indicators_data = ["blood_glucose", "heart_rate", "body_temperature", "hemoglobin"]

            logging.info("📊 Input data:")
            logging.info(json.dumps(indicators_data, ensure_ascii=False, indent=2))

            # Execute batch classification
            batch_results = await classifier.classify_indicators_batch(indicators_data)

            logging.info("\n📊 Batch analysis results:")
            logging.info(json.dumps(batch_results, ensure_ascii=False, indent=2))

            logging.info("\n" + "=" * 50)
            logging.info("🎉 Demo completed!")

            logging.info("\n💡 Usage:")
            logging.info("🔹 Single indicator classification:")
            logging.info("  results = await classifier.classify_indicators_batch(['blood_pressure'])")
            logging.info("  result = results[0]  # Get first result")
            logging.info("")
            logging.info("🔹 Batch indicator classification:")
            logging.info("  indicators = ['blood_glucose', 'heart_rate', 'body_temperature', 'hemoglobin']")
            logging.info("  results = await classifier.classify_indicators_batch(indicators)")
            logging.info("  # Returns list containing input info and classification results")

        except Exception as e:
            logging.error(f"Example execution failed: {e}")
            logging.error(traceback.format_exc())

    # Run async main function
    asyncio.run(main())
