"""
Recipe Registry & Matcher

Manages recipe registration and matches recipes to user profiles.

Usage:
    registry = RecipeRegistry()
    registry.register(recipe)
    matched = registry.match(profile)
"""

import logging
from typing import Dict, List

from .models import DensityLevel, InsightRecipe, UserProfile


class RecipeRegistry:
    """Registry for insight recipes with matching logic."""

    def __init__(self):
        self._recipes: Dict[str, InsightRecipe] = {}

    def register(self, recipe: InsightRecipe) -> None:
        """Register a recipe."""
        self._recipes[recipe.name] = recipe
        logging.info(f"[RecipeRegistry] Registered recipe: {recipe.name} v{recipe.version}")

    def get(self, name: str) -> InsightRecipe:
        """Get a recipe by name."""
        return self._recipes.get(name)

    def all(self) -> List[InsightRecipe]:
        """Return all registered recipes."""
        return list(self._recipes.values())

    def match(self, profile: UserProfile) -> List[InsightRecipe]:
        """Find all recipes that the user's data can support.

        Matching criteria:
        1. All required_categories must be in profile.available_categories
        2. Each required category's density >= min_density_days
        3. Overlap days across required categories >= min_overlap_days

        Args:
            profile: UserProfile from BaselineEngine

        Returns:
            List of matched InsightRecipe objects
        """
        matched = []

        for recipe in self._recipes.values():
            if self._matches(recipe, profile):
                matched.append(recipe)

        logging.info(
            f"[RecipeRegistry] user={profile.user_id} "
            f"matched {len(matched)}/{len(self._recipes)} recipes: "
            f"{[r.name for r in matched]}"
        )
        return matched

    def _matches(self, recipe: InsightRecipe, profile: UserProfile) -> bool:
        """Check if a single recipe matches the user profile."""
        available = set(profile.available_categories)

        # Check required categories
        for cat in recipe.required_categories:
            if cat not in available:
                return False

        # Check density for each required category
        for cat in recipe.required_categories:
            density = profile.densities.get(cat)
            if not density or density.days_with_data < recipe.min_density_days:
                return False

        # Check overlap: count days where ALL required categories have data
        # This is approximated by the minimum density across required categories
        if len(recipe.required_categories) > 1:
            min_density = min(
                profile.densities[cat].days_with_data
                for cat in recipe.required_categories
                if cat in profile.densities
            )
            if min_density < recipe.min_overlap_days:
                return False

        return True


# Global singleton
recipe_registry = RecipeRegistry()
