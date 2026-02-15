document.addEventListener('DOMContentLoaded', () => {
  // Initialize Lenis for smooth scrolling
  if (typeof Lenis !== 'undefined') {
    const lenis = new Lenis({
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
      lenis.raf(time);
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
        y: 50,
        opacity: 0,
        duration: 1,
        stagger: 0.1,
        ease: 'power3.out',
        delay: 0.2
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
    // Feature cards also have .animate-fade-up (opacity: 0 in CSS).
    // Use gsap.to to animate them TO visibility with stagger.
    const featureCards = document.querySelectorAll('.feature-card');
    if (featureCards.length) {
      gsap.to(featureCards, {
        scrollTrigger: {
          trigger: '.features-grid',
          start: 'top 80%',
        },
        y: 0,
        opacity: 1,
        duration: 0.8,
        stagger: 0.1,
        ease: 'power2.out'
      });
    }
  }

  // Mobile Nav Toggle
  const navToggle = document.getElementById('navToggle');
  const navLinks = document.getElementById('navLinks');

  if (navToggle && navLinks) {
    navToggle.addEventListener('click', () => {
      navLinks.classList.toggle('active');
      navToggle.classList.toggle('active');
    });
  }

  // Copy Code Functionality
  document.querySelectorAll('.code-copy').forEach(button => {
    button.addEventListener('click', () => {
      const codeBlock = button.closest('.code-block');
      const code = codeBlock.querySelector('code').innerText;

      navigator.clipboard.writeText(code).then(() => {
        const originalText = button.textContent;
        button.textContent = 'Copied!';
        button.classList.add('copied');

        setTimeout(() => {
          button.textContent = originalText;
          button.classList.remove('copied');
        }, 2000);
      });
    });
  });

  // Active Link Highlighting (Sidebar)
  const sections = document.querySelectorAll('h2[id], h3[id]');
  const navItems = document.querySelectorAll('.docs-nav-link');

  if (sections.length > 0 && navItems.length > 0) {
      window.addEventListener('scroll', () => {
        let current = '';
        sections.forEach(section => {
            const sectionTop = section.offsetTop;
            // Adjustment for fixed header
            if (window.scrollY >= (sectionTop - 150)) {
                current = section.getAttribute('id');
            }
        });

        if (current) {
            navItems.forEach(li => {
                li.classList.remove('active');
                if (li.getAttribute('href') === `#${current}`) {
                    li.classList.add('active');
                }
            });
        }
      });
  }
});
