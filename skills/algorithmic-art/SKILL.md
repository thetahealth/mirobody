---
name: algorithmic-art
description: Creating algorithmic art using p5.js with seeded randomness and interactive parameter exploration. Use when users request creating art using code, generative art, algorithmic art, flow fields, or particle systems.
source: anthropics/skills
license: Apache-2.0
---

# Algorithmic Art

Create generative art with code using p5.js, featuring seeded randomness for reproducibility.

## Core Concepts

### Seeded Randomness
```javascript
// Use seed for reproducible results
function setup() {
  randomSeed(42);
  noiseSeed(42);
}
```

### Noise Functions
```javascript
// Perlin noise for organic patterns
let x = noise(frameCount * 0.01) * width;
let y = noise(frameCount * 0.01 + 1000) * height;
```

## Common Patterns

### Flow Fields
```javascript
let cols, rows, scale = 20;
let particles = [];
let flowfield;

function setup() {
  createCanvas(800, 800);
  cols = floor(width / scale);
  rows = floor(height / scale);
  flowfield = new Array(cols * rows);

  for (let i = 0; i < 1000; i++) {
    particles.push(new Particle());
  }
}

function draw() {
  let yoff = 0;
  for (let y = 0; y < rows; y++) {
    let xoff = 0;
    for (let x = 0; x < cols; x++) {
      let angle = noise(xoff, yoff) * TWO_PI * 2;
      let v = p5.Vector.fromAngle(angle);
      flowfield[x + y * cols] = v;
      xoff += 0.1;
    }
    yoff += 0.1;
  }

  particles.forEach(p => {
    p.follow(flowfield);
    p.update();
    p.show();
  });
}
```

### Recursive Trees
```javascript
function branch(len) {
  line(0, 0, 0, -len);
  translate(0, -len);

  if (len > 4) {
    push();
    rotate(PI / 6);
    branch(len * 0.67);
    pop();

    push();
    rotate(-PI / 6);
    branch(len * 0.67);
    pop();
  }
}
```

### Particle Systems
```javascript
class Particle {
  constructor() {
    this.pos = createVector(random(width), random(height));
    this.vel = createVector(0, 0);
    this.acc = createVector(0, 0);
    this.maxSpeed = 4;
  }

  follow(flowfield) {
    let x = floor(this.pos.x / scale);
    let y = floor(this.pos.y / scale);
    let force = flowfield[x + y * cols];
    this.acc.add(force);
  }

  update() {
    this.vel.add(this.acc);
    this.vel.limit(this.maxSpeed);
    this.pos.add(this.vel);
    this.acc.mult(0);
  }

  show() {
    stroke(255, 5);
    point(this.pos.x, this.pos.y);
  }
}
```

## Color Palettes

```javascript
// Define palette
const palette = ['#264653', '#2a9d8f', '#e9c46a', '#f4a261', '#e76f51'];

// Random from palette
fill(random(palette));
```

## Best Practices

- Use `noLoop()` for static pieces, save with `save('art.png')`
- Experiment with blend modes: `blendMode(ADD)`
- Layer transparency for depth
- Use frameCount for animation
