document.addEventListener('DOMContentLoaded', () => {
  // Initialize Lenis for smooth scrolling (Lenis natively handles touch tracking safely now)
  if (typeof Lenis !== 'undefined') {
    window.lenis = new Lenis({
      duration: 1.2,
      easing: (t) => Math.min(1, 1.001 - Math.pow(2, -10 * t)),
      direction: 'vertical',
      gestureDirection: 'vertical',
      smooth: true,
      mouseMultiplier: 1,
      smoothTouch: false,
      touchMultiplier: 2,
    });

    function raf(time) {
      window.lenis.raf(time);
      requestAnimationFrame(raf);
    }

    requestAnimationFrame(raf);
  }

  // GSAP Animations
  if (typeof gsap !== 'undefined' && typeof ScrollTrigger !== 'undefined') {
    gsap.registerPlugin(ScrollTrigger);

    // Hero Text Reveal
    // These elements start visible in CSS, so gsap.from works (hides them then animates in)
    const heroElements = document.querySelectorAll('.hero-title, .hero-subtitle, .hero-cta, .hero-stats');
    if (heroElements.length) {
      gsap.from(heroElements, {
        y: 30,
        opacity: 0,
        duration: 0.8,
        stagger: 0.1,
        ease: 'power2.out',
        delay: 0.2,
        clearProps: 'opacity,transform' // Ensure no residue
      });
    }

    // General Fade In Elements
    // These elements have .animate-fade-up which sets opacity: 0 in CSS.
    // We must use gsap.to to animate them TO visibility.
    // We exclude .feature-card to handle them separately with stagger.
    const fadeElements = document.querySelectorAll('.animate-fade-up:not(.feature-card)');
    fadeElements.forEach(el => {
      gsap.to(el, {
        scrollTrigger: {
          trigger: el,
          start: 'top 85%',
          toggleActions: 'play none none reverse'
        },
        y: 0,
        opacity: 1,
        duration: 0.8,
        ease: 'power2.out'
      });
    });

    // Section Headers Reveal
    gsap.utils.toArray('.section-header').forEach(header => {
      gsap.from(header, {
        scrollTrigger: {
          trigger: header,
          start: 'top 80%',
          toggleActions: 'play none none reverse'
        },
        y: 30,
        opacity: 0,
        duration: 0.8,
        ease: 'power2.out'
      });
    });

    // Feature Cards Stagger
    const featureCards = document.querySelectorAll('.feature-card');
    if (featureCards.length) {
      gsap.to(featureCards, {
        scrollTrigger: {
          trigger: '.feature-grid-parallax',
          start: 'top 80%',
        },
        y: 0,
        opacity: 1,
        duration: 0.8,
        stagger: 0.15,
        ease: 'power3.out'
      });
    }

    // Parallax Decorative Elements optimized
    gsap.utils.toArray('.bg-text').forEach((el, i) => {
      gsap.to(el, {
        scrollTrigger: {
          trigger: el.parentElement,
          start: 'top bottom',
          end: 'bottom top',
          scrub: 1 // reduced from 1.5 for performance
        },
        x: i % 2 === 0 ? 100 : -100, // Reduced translation amount
        ease: 'none',
        force3D: true // Hardware acceleration
      });
    });

    // Floating Code Elements
    gsap.utils.toArray('.floating-code').forEach((el, i) => {
      // Small random float
      gsap.to(el, {
        y: (i + 1) * -40,
        x: (i % 2 === 0 ? 20 : -20),
        duration: 4 + i,
        repeat: -1,
        yoyo: true,
        ease: 'sine.inOut',
        delay: i * 0.2
      });

      // Scroll Parallax
      gsap.to(el, {
        scrollTrigger: {
          trigger: '.parallax-hero',
          start: 'top top',
          end: 'bottom top',
          scrub: true
        },
        y: (i + 1) * -150,
        opacity: 0,
        ease: 'none'
      });
    });

    // Hero Code Window Parallax
    const codeWindow = document.querySelector('.hero-code-window');
    if (codeWindow) {
      gsap.to(codeWindow, {
        scrollTrigger: {
          trigger: '.parallax-hero',
          start: 'top top',
          end: 'bottom top',
          scrub: true
        },
        y: 50,
        opacity: 0.5, // Start from semi-visible
        scale: 0.95,
        duration: 0.8,
        ease: 'power2.out',
        overwrite: true
      });
    }

    // Keeping title stable for performance and readability
  }

  // Glare Effect Logic
  const glareContainers = document.querySelectorAll('.glare-container');
  glareContainers.forEach(container => {
    const glare = container.querySelector('.glare');
    
    container.addEventListener('mousemove', (e) => {
      const rect = container.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      
      container.style.setProperty('--mouse-x', `${x}px`);
      container.style.setProperty('--mouse-y', `${y}px`);
    });
  });

  // Ultra-Responsive Magnetic Button Effect
  if (typeof gsap !== 'undefined') {
    const magneticButtons = document.querySelectorAll('.btn, .nav-cta, .hero-badge');
    magneticButtons.forEach(btn => {
      btn.addEventListener('mousemove', (e) => {
        const rect = btn.getBoundingClientRect();
        const centerX = rect.left + rect.width / 2;
        const centerY = rect.top + rect.height / 2;
        
        const x = (e.clientX - centerX) * 0.15;
        const y = (e.clientY - centerY) * 0.15;
        
        gsap.to(btn, {
          x: x,
          y: y,
          duration: 0.5,
          ease: "power2.out",
          overwrite: "auto"
        });
      });
      
      btn.addEventListener('mouseleave', () => {
        gsap.to(btn, {
          x: 0,
          y: 0,
          duration: 1.5,
          ease: "elastic.out(1, 0.3)",
          overwrite: "auto"
        });
      });
    });
  }

  // Staggered Reveal for Feature Cards
  const featureCards = document.querySelectorAll('.feature-card');
  if (featureCards.length) {
    gsap.set(featureCards, { y: 40, opacity: 0 });
    
    ScrollTrigger.batch(featureCards, {
      onEnter: batch => gsap.to(batch, { opacity: 1, y: 0, stagger: 0.1, duration: 0.8, ease: 'power2.out', overwrite: true }),
      onLeaveBack: batch => gsap.set(batch, { opacity: 0, y: 40, overwrite: true }),
      start: 'top 90%'
    });
  }

  // Mobile Nav Toggle
  const navToggle = document.getElementById('navToggle');
  const navLinks = document.getElementById('navLinks');

  if (navToggle && navLinks) {
    navToggle.addEventListener('click', () => {
      navLinks.classList.toggle('active');
      navToggle.classList.toggle('active');
      // Prevent body scroll when menu is open
      document.body.style.overflow = navLinks.classList.contains('active') ? 'hidden' : '';
    });
    
    // Close menu when a link is clicked
    navLinks.querySelectorAll('a').forEach(link => {
      link.addEventListener('click', () => {
        navLinks.classList.remove('active');
        navToggle.classList.remove('active');
        document.body.style.overflow = '';
      });
    });
  }

  // Copy Code Functionality
  document.querySelectorAll('.code-copy, .hero-install-cmd').forEach(button => {
    button.addEventListener('click', () => {
      let code = '';
      if (button.classList.contains('hero-install-cmd')) {
        code = button.querySelector('.cmd-text').innerText;
      } else {
        const codeBlock = button.closest('.code-block');
        code = codeBlock.querySelector('code').innerText;
      }

      navigator.clipboard.writeText(code).then(() => {
        const originalContent = button.innerHTML;
        if (button.classList.contains('hero-install-cmd')) {
          const cmdText = button.querySelector('.cmd-text');
          const oldText = cmdText.innerText;
          cmdText.innerText = 'Copied!';
          setTimeout(() => cmdText.innerText = oldText, 2000);
        } else {
          button.textContent = 'Copied!';
          button.classList.add('copied');
          setTimeout(() => {
            button.innerHTML = originalContent;
            button.classList.remove('copied');
          }, 2000);
        }
      });
    });
  });

  // Active highlighting is handled by IntersectionObserver in docs.html for better performance

});
