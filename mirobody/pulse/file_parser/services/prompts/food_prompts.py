FOOD_RECOGNIZE_PROMPT = """
My health needs and underlying conditions are: {query} .
Based on my needs, try your best to analyze the image and determine if it contains food or drinks (including water). If it contains food or drinks, output the approximation of nutrition composition of the contained foods/drinks.
Firstly you should generate your thought process of the analysis, then output according to the specified JSON schema format.

Requirements:
- is_food: determine if the image contains food or drinks ("1" for yes, "0" for no). Note: water, beverages, and all consumable items should be considered as food. If no food/drink is detected, only return is_food="0" and title fields.  
- title: a descriptive title for the analysis (e.g., "Here are the key numbers to watch in your diet:" for food, or "No food detected in this image" for non-food)
- name: food name (e.g., "Japanese Bento", "Water") - only required when is_food="1"
- category: meal category based on food type and portion size ("breakfast", "lunch", "dinner", or "snack") - only required when is_food="1"
- advice: recommendations for the entire food portion - only required when is_food="1"
- nut: overall nutritional composition of the entire food portion - only required when is_food="1", with each element containing:
  - n: nutrient name
  - v: value (rounded to one decimal place)  
  - u: unit of measurement (e.g., "kcal", "g", "mg")
  - s: string representation of value with unit (e.g., '100.0kcal')
  - c: nutrient health level indicator (green for healthy, yellow for moderate, red for unhealthy)
  - t: type of nutritional element, must be one of [Calorie, Protein, Carbs, Fat, Salt, Sugar, Vitamin, Histamine, Purine, Other]
  - sn: suggested healthy value of this nutritional, string representation of value with unit (e.g., '100.0kcal')
- nutrition: composition of individual food components - only required when is_food="1", where each item has name and nut fields

Note: All nutritional elements are measured per 100g, and suggested nutrients (sn) are per day.
Note: For category classification:
  - "breakfast": morning meals, light foods, cereals, fruits, coffee, etc.
  - "lunch": midday meals, balanced portions, main dishes with sides
  - "dinner": evening meals, substantial portions, complete meals
  - "snack": small portions, finger foods, desserts, beverages, light bites

Please output all content in {language} language.
"""

FOOD_RECOGNIZE_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "is_food": {
            "type": "string",
            "enum": ["0", "1"],
            "description": "Whether the image contains food or drinks ('1' for yes, '0' for no)",
        },
        "title": {
            "type": "string",
            "description": "Descriptive title for the analysis",
        },
        "name": {
            "type": "string",
            "description": "Name of the food item (only when is_food=1)",
        },
        "category": {
            "type": "string",
            "enum": ["breakfast", "lunch", "dinner", "snack"],
            "description": "Meal category based on food type and portion size",
        },
        "nut": {
            "type": "array",
            "description": "Overall nutritional composition of the entire food portion",
            "items": {
                "type": "object",
                "properties": {
                    "n": {"type": "string", "description": "Nutrient name"},
                    "t": {
                        "type": "string",
                        "enum": [
                            "Calorie",
                            "Protein",
                            "Carbs",
                            "Fat",
                            "Salt",
                            "Sugar",
                            "Vitamin",
                            "Histamine",
                            "Purine",
                            "Other",
                        ],
                        "description": "Type of nutritional element",
                    },
                    "v": {
                        "type": "number",
                        "description": "Value (rounded to one decimal place)",
                    },
                    "u": {
                        "type": "string",
                        "description": "Unit of measurement (e.g., 'kcal', 'g', 'mg')",
                    },
                    "s": {
                        "type": "string",
                        "description": "String representation of value with unit",
                    },
                    "c": {
                        "type": "string",
                        "enum": ["green", "yellow", "red"],
                        "description": "Health level indicator",
                    },
                    "sn": {
                        "type": "string",
                        "description": "Suggested healthy value with unit",
                    },
                },
                "required": ["n", "t", "v", "u", "s", "c", "sn"],
            },
        },
        "advice": {
            "type": "array",
            "description": "Recommendations for the entire food portion",
            "items": {"type": "string"},
        },
        "nutrition": {
            "type": "array",
            "description": "Composition of individual food components",
            "items": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the food component",
                    },
                    "nut": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "n": {"type": "string", "description": "Nutrient name"},
                                "t": {
                                    "type": "string",
                                    "enum": [
                                        "Calorie",
                                        "Protein",
                                        "Carbs",
                                        "Fat",
                                        "Salt",
                                        "Sugar",
                                        "Vitamin",
                                        "Histamine",
                                        "Purine",
                                        "Other",
                                    ],
                                    "description": "Type of nutritional element",
                                },
                                "v": {
                                    "type": "number",
                                    "description": "Value (rounded to one decimal place)",
                                },
                                "u": {
                                    "type": "string",
                                    "description": "Unit of measurement (e.g., 'kcal', 'g', 'mg')",
                                },
                                "s": {
                                    "type": "string",
                                    "description": "String representation of value with unit",
                                },
                                "c": {
                                    "type": "string",
                                    "enum": ["green", "yellow", "red"],
                                    "description": "Health level indicator",
                                },
                                "sn": {
                                    "type": "string",
                                    "description": "Suggested healthy value with unit",
                                },
                            },
                            "required": ["n", "t", "v", "u", "s", "c", "sn"],
                        },
                    },
                },
                "required": ["name", "nut"],
            },
        },
    },
    "required": ["is_food", "title"],
}


# Streamlined prompt for Doubao model - more concise and efficient
SIMPLE_FOOD_RECOGNIZE_PROMPT = """Based on my health needs: {query}

Analyze the image to determine if it contains food/drinks and extract nutrition information. Return JSON format response in {language}.

Key requirements:
- is_food: "1" for food/drinks detected, "0" for no food (include water and all consumables)
- For food detected: provide name, category (breakfast/lunch/dinner/snack), nutrition analysis, and health advice
- For non-food: only return is_food="0" and title
- All nutrition values per 100g, suggested values per day
- Health indicators: green (healthy), yellow (moderate), red (unhealthy)

JSON structure:
{{
  "is_food": "1/0",
  "title": "Analysis title",
  "name": "Food name" (if is_food=1),
  "category": "breakfast/lunch/dinner/snack" (if is_food=1),
  "nut": [{{ "n": "nutrient name", "t": "Calorie/Protein/Carbs/Fat/Salt/Sugar/Vitamin/Histamine/Purine/Other", "v": value, "u": "unit", "s": "value+unit", "c": "green/yellow/red", "sn": "suggested value+unit" }}],
  "advice": ["health recommendations"],
  "nutrition": [{{ "name": "component name", "nut": [same structure as above] }}]
}}

Categories: breakfast (morning/light foods), lunch (midday/balanced meals), dinner (evening/substantial meals), snack (small portions/beverages)"""
