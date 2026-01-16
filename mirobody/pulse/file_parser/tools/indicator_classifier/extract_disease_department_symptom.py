#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract disease names, departments, and symptoms mapping from medical.json file
Output format: disease_name(name) -> department(cure_department) -> symptoms(symptom)
"""

import json
import os
from datetime import datetime


class DiseaseDataExtractor:
    def __init__(self):
        self.input_file = "json_data/medical.json"
        self.output_dir = "output"

    def load_medical_data(self):
        """Read medical.json file"""
        try:
            print(f"📁 Reading medical.json file: {self.input_file}")

            # Due to large file size, process line by line
            diseases = []
            with open(self.input_file, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        # Parse JSON data for each line
                        disease_data = json.loads(line)
                        diseases.append(disease_data)

                        # Display progress
                        if line_num % 1000 == 0:
                            print(f"  Processed {line_num} lines...")

                    except json.JSONDecodeError as e:
                        print(f"⚠️  JSON parsing error on line {line_num}: {e}")
                        continue

            print(f"✅ Successfully read {len(diseases)} disease records")
            return diseases

        except FileNotFoundError:
            print(f"❌ File not found: {self.input_file}")
            return None
        except Exception as e:
            print(f"❌ Failed to read file: {e}")
            return None

    def extract_disease_info(self, diseases):
        """Extract disease names, departments, and symptoms information"""
        print("\n🔍 Extracting disease information...")

        disease_info = []
        valid_count = 0
        invalid_count = 0

        for disease in diseases:
            try:
                # Extract basic information
                name = disease.get("name", "").strip()
                cure_department = disease.get("cure_department", [])
                symptom = disease.get("symptom", [])

                # Validate required fields
                if not name:
                    invalid_count += 1
                    continue

                # Ensure cure_department and symptom are lists
                if not isinstance(cure_department, list):
                    cure_department = [cure_department] if cure_department else []
                if not isinstance(symptom, list):
                    symptom = [symptom] if symptom else []

                # Filter empty values
                cure_department = [dept.strip() for dept in cure_department if dept and dept.strip()]
                symptom = [symp.strip() for symp in symptom if symp and symp.strip()]

                disease_info.append(
                    {
                        "name": name,
                        "cure_department": cure_department,
                        "symptom": symptom,
                    }
                )

                valid_count += 1

            except Exception as e:
                print(f"⚠️  Error processing disease data: {e}")
                invalid_count += 1
                continue

        print("📊 Data extraction results:")
        print(f"  ✅ Valid data: {valid_count}")
        print(f"  ❌ Invalid data: {invalid_count}")
        print(f"  📝 Total: {len(disease_info)}")

        return disease_info

    def generate_statistics(self, disease_info):
        """Generate statistics"""
        print("\n📈 Generating statistics...")

        # Count departments
        all_departments = []
        for disease in disease_info:
            all_departments.extend(disease["cure_department"])

        department_count = {}
        for dept in all_departments:
            department_count[dept] = department_count.get(dept, 0) + 1

        # Count symptoms
        all_symptoms = []
        for disease in disease_info:
            all_symptoms.extend(disease["symptom"])

        symptom_count = {}
        for symp in all_symptoms:
            symptom_count[symp] = symptom_count.get(symp, 0) + 1

        # Count diseases with departments but no symptoms
        no_symptom_count = sum(1 for disease in disease_info if disease["cure_department"] and not disease["symptom"])

        # Count diseases with symptoms but no departments
        no_department_count = sum(
            1 for disease in disease_info if disease["symptom"] and not disease["cure_department"]
        )

        # Count diseases with complete data
        complete_count = sum(1 for disease in disease_info if disease["cure_department"] and disease["symptom"])

        stats = {
            "total_diseases": len(disease_info),
            "total_departments": len(department_count),
            "total_symptoms": len(symptom_count),
            "department_count": department_count,
            "symptom_count": symptom_count,
            "no_symptom_count": no_symptom_count,
            "no_department_count": no_department_count,
            "complete_count": complete_count,
        }

        return stats

    def save_disease_info(self, disease_info, stats):
        """Save disease information to JSON file"""
        try:
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)

            # Generate output filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = "disease_department_symptom.json"
            output_path = os.path.join(self.output_dir, output_filename)

            # Prepare output data
            output_data = {
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "source_file": self.input_file,
                    "statistics": stats,
                },
                "diseases": disease_info,
            }

            # Save JSON file
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            print("\n✅ Disease information saved:")
            print(f"  📁 File path: {output_path}")
            print(f"  📊 File size: {os.path.getsize(output_path) / (1024 * 1024):.2f} MB")

            return output_path

        except Exception as e:
            print(f"❌ Failed to save file: {e}")
            return None

    def print_statistics(self, stats):
        """Print statistics"""
        print("\n📊 Detailed statistics:")
        print(f"  🏥 Total diseases: {stats['total_diseases']}")
        print(f"  🏥 Total departments: {stats['total_departments']}")
        print(f"  🏥 Total symptoms: {stats['total_symptoms']}")
        print(f"  ✅ Complete data diseases: {stats['complete_count']}")
        print(f"  ⚠️  Diseases without symptoms: {stats['no_symptom_count']}")
        print(f"  ⚠️  Diseases without departments: {stats['no_department_count']}")

        print("\n🏥 Department distribution (Top 10):")
        sorted_departments = sorted(stats["department_count"].items(), key=lambda x: x[1], reverse=True)
        for i, (dept, count) in enumerate(sorted_departments[:10], 1):
            print(f"  {i:2d}. {dept}: {count}")

        print("\n🏥 Symptom distribution (Top 10):")
        sorted_symptoms = sorted(stats["symptom_count"].items(), key=lambda x: x[1], reverse=True)
        for i, (symptom, count) in enumerate(sorted_symptoms[:10], 1):
            print(f"  {i:2d}. {symptom}: {count}")

    def show_sample_data(self, disease_info, count=5):
        """Show sample data"""
        print(f"\n📝 Sample data (First {count} records):")
        for i, disease in enumerate(disease_info[:count], 1):
            print(f"\n  {i}. Disease name: {disease['name']}")
            print(f"     Department: {', '.join(disease['cure_department']) if disease['cure_department'] else 'None'}")
            print(f"     Symptoms: {', '.join(disease['symptom']) if disease['symptom'] else 'None'}")

    def process(self):
        """Main processing workflow"""
        print("=" * 80)
        print("🏥 Disease-Department-Symptom Data Extraction Tool")
        print("=" * 80)

        # 1. Read medical.json file
        diseases = self.load_medical_data()
        if not diseases:
            return False

        # 2. Extract disease information
        disease_info = self.extract_disease_info(diseases)
        if not disease_info:
            print("❌ No valid disease information extracted")
            return False

        # 3. Generate statistics
        stats = self.generate_statistics(disease_info)

        # 4. Display statistics
        self.print_statistics(stats)

        # 5. Display sample data
        self.show_sample_data(disease_info)

        # 6. Confirm save
        print(f"\n⚠️  About to save {len(disease_info)} disease records")
        confirm = input("❓ Confirm save to JSON file? (y/N): ").strip().lower()
        if confirm != "y":
            print("❌ Operation cancelled")
            return False

        # 7. Save data
        output_path = self.save_disease_info(disease_info, stats)

        if output_path:
            print("\n🎉 Data processing completed!")
            print(f"📁 Output file: {output_path}")
            return True
        else:
            print("\n❌ Data processing failed!")
            return False


def main():
    """Main function"""
    extractor = DiseaseDataExtractor()

    try:
        success = extractor.process()
        if success:
            print("\n✅ Script executed successfully!")
        else:
            print("\n❌ Script execution failed!")

    except KeyboardInterrupt:
        print("\n❌ Operation interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
