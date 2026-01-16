---
name: canvas-design
description: Create beautiful visual art in .png and .pdf documents using design philosophy. Use when the user asks to create a poster, piece of art, design, or other static visual piece. Creates original visual designs.
source: anthropics/skills
license: Apache-2.0
---

# Canvas Design

Create visually striking static designs using HTML Canvas or Python imaging libraries.

## Design Principles

### Composition
- **Rule of Thirds**: Place key elements along grid lines
- **Visual Hierarchy**: Size, color, and position indicate importance
- **White Space**: Embrace negative space for elegance
- **Balance**: Symmetrical for formal, asymmetrical for dynamic

### Color Theory
- **Complementary**: Colors opposite on wheel (high contrast)
- **Analogous**: Adjacent colors (harmonious)
- **Triadic**: Three equidistant colors (vibrant)
- Limit palette to 3-5 colors

### Typography
- Pair one display font with one body font
- Maintain consistent hierarchy
- Ensure readability (contrast, size)

## Python Canvas (Pillow + Cairo)

```python
from PIL import Image, ImageDraw, ImageFont
import cairo

# Create canvas
width, height = 1200, 800
surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
ctx = cairo.Context(surface)

# Background gradient
pattern = cairo.LinearGradient(0, 0, 0, height)
pattern.add_color_stop_rgb(0, 0.1, 0.1, 0.2)
pattern.add_color_stop_rgb(1, 0.05, 0.05, 0.1)
ctx.set_source(pattern)
ctx.paint()

# Draw shapes
ctx.set_source_rgba(1, 0.3, 0.3, 0.8)
ctx.arc(600, 400, 150, 0, 2 * 3.14159)
ctx.fill()

# Add text
ctx.set_source_rgb(1, 1, 1)
ctx.select_font_face("Sans", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
ctx.set_font_size(48)
ctx.move_to(400, 600)
ctx.show_text("Hello Design")

# Save
surface.write_to_png("design.png")
```

## HTML Canvas to Image

```javascript
const canvas = document.createElement('canvas');
canvas.width = 1200;
canvas.height = 800;
const ctx = canvas.getContext('2d');

// Draw
ctx.fillStyle = '#1a1a2e';
ctx.fillRect(0, 0, 1200, 800);

ctx.fillStyle = '#e94560';
ctx.beginPath();
ctx.arc(600, 400, 150, 0, Math.PI * 2);
ctx.fill();

// Export
const dataUrl = canvas.toDataURL('image/png');
```

## Design Styles

- **Minimalist**: Limited colors, lots of whitespace, clean lines
- **Brutalist**: Raw, bold typography, stark contrasts
- **Glassmorphism**: Frosted glass effects, subtle borders
- **Retro/Vintage**: Muted colors, textures, classic typography
- **Abstract**: Geometric shapes, gradients, artistic composition
