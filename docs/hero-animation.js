/**
 * Hero Animation: Digital Warp Speed
 * Description: Renders a high-speed data tunnel effect using HTML5 Canvas.
 * Design: Centered vanishing point, dynamic speed on scroll, glowing trails.
 */

const initHeroAnimation = () => {
  const canvas = document.getElementById('hero-canvas');
  if (!canvas) return;

  const ctx = canvas.getContext('2d');
  let width = window.innerWidth;
  let height = window.innerHeight;
  
  // Set canvas size
  canvas.width = width;
  canvas.height = height;

  // Configuration
  const STAR_COUNT = 400;
  const BASE_SPEED = 5;
  const BOOST_SPEED = 40;
  let targetSpeed = BASE_SPEED;
  let currentSpeed = BASE_SPEED;
  
  let centerX = width / 2;
  let centerY = height / 2;
  const fov = 300; // Field of View (Depth)

  const colors = [
    '#D4FF00', // Acid Green (Primary)
    '#E5FF4D', // Light Acid
    '#A6C700', // Darker Acid
    '#FFFFFF', // White
    '#F0FFB2'  // Pale Acid
  ];

  class Star {
    constructor() {
      this.init(true);
    }

    init(randomZ = false) {
      // Spread stars widely to ensure tunnel is filled even at edges
      // We use a large multiplier to cover the screen even when projected
      this.x = (Math.random() - 0.5) * width * 4;
      this.y = (Math.random() - 0.5) * height * 4;
      
      // Z depth: randomZ allows initialization at various depths, otherwise start at far end
      this.z = randomZ ? Math.random() * width : width;
      
      // Random properties
      this.color = colors[Math.floor(Math.random() * colors.length)];
      this.sizeBase = Math.random() * 2 + 0.5;
    }

    update() {
      // Move closer (decrease Z)
      this.z -= currentSpeed;
      
      // Reset if passed viewer (Z <= 1)
      if (this.z <= 1) {
        this.init(false);
      }
    }

    draw() {
      // Perspective Projection: sx = x * (fov / z) + centerX
      if (this.z <= 0) return;
      
      const scale = fov / this.z;
      const sx = this.x * scale + centerX;
      const sy = this.y * scale + centerY;
      
      // Calculate previous position for trail (based on speed)
      // We look back in Z to see where it was a moment ago
      const prevZ = this.z + currentSpeed * 2; 
      const prevScale = fov / prevZ;
      const px = this.x * prevScale + centerX;
      const py = this.y * prevScale + centerY;

      // Don't draw if clearly off-screen (optimization)
      if (sx < -100 || sx > width + 100 || sy < -100 || sy > height + 100) return;

      // Opacity fades as it gets closer (optional) or stays solid?
      // Let's fade in from distance and fade out when too close (clipping plane feel)
      let alpha = 1;
      if (this.z > width * 0.8) {
          alpha = (width - this.z) / (width * 0.2);
      }
      
      const size = this.sizeBase * scale;

      ctx.beginPath();
      ctx.moveTo(px, py);
      ctx.lineTo(sx, sy);
      
      ctx.strokeStyle = this.color;
      ctx.lineWidth = size;
      ctx.globalAlpha = alpha;
      ctx.lineCap = 'round';
      ctx.stroke();
      ctx.globalAlpha = 1.0;
    }
  }

  // Create Stars
  const stars = [];
  for (let i = 0; i < STAR_COUNT; i++) {
    stars.push(new Star());
  }

  function animate() {
    // Clear with trail effect (semi-transparent fill)
    // Using darker fill for more contrast
    ctx.fillStyle = 'rgba(5, 5, 5, 0.4)'; 
    ctx.fillRect(0, 0, width, height);

    // Smooth speed transition logic
    if (targetSpeed > currentSpeed) {
        // Accelerate fast
        currentSpeed += (targetSpeed - currentSpeed) * 0.1;
    } else {
        // Decelerate slowly
        currentSpeed += (targetSpeed - currentSpeed) * 0.05;
    }
    
    // Auto-decay spread back to base
    if (targetSpeed > BASE_SPEED) {
        targetSpeed -= 0.5;
        if (targetSpeed < BASE_SPEED) targetSpeed = BASE_SPEED;
    }

    stars.forEach(star => {
      star.update();
      star.draw();
    });

    requestAnimationFrame(animate);
  }

  // Start Animation
  animate();

  // Resize Handler
  window.addEventListener('resize', () => {
    width = window.innerWidth;
    height = window.innerHeight;
    canvas.width = width;
    canvas.height = height;
    centerX = width / 2;
    centerY = height / 2;
  });

  // Interaction: Boost on scroll
  let scrollTimeout;
  window.addEventListener('scroll', () => {
    targetSpeed = BOOST_SPEED;
    clearTimeout(scrollTimeout);
    scrollTimeout = setTimeout(() => {
        // Just let it decay naturally
    }, 100);
  });
  
  // Interaction: Mouse Parallax
  window.addEventListener('mousemove', (e) => {
    // Subtle shift of vanishing point based on mouse position
    const targetX = (width / 2) + (e.clientX - width / 2) * 0.5;
    const targetY = (height / 2) + (e.clientY - height / 2) * 0.5;
    
    centerX += (targetX - centerX) * 0.05;
    centerY += (targetY - centerY) * 0.05;
  });
};

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initHeroAnimation);
} else {
    initHeroAnimation();
}
