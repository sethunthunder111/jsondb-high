// ============================================
// JSONDB-HIGH DOCS - MAIN SCRIPT
// ============================================

// Initialize Lenis smooth scrolling
const lenis = new Lenis({
    duration: 1.2,
    easing: (t) => Math.min(1, 1.001 - Math.pow(2, -10 * t)),
    orientation: 'vertical',
    gestureOrientation: 'vertical',
    smoothWheel: true,
    wheelMultiplier: 1,
    touchMultiplier: 2,
    infinite: false,
});

function raf(time) {
    lenis.raf(time);
    requestAnimationFrame(raf);
}

requestAnimationFrame(raf);

// ============================================
// NAVIGATION
// ============================================
const nav = document.getElementById('nav');
const navToggle = document.getElementById('navToggle');
const mobileMenu = document.getElementById('mobileMenu');

// Scroll detection for nav
let lastScroll = 0;
window.addEventListener('scroll', () => {
    const currentScroll = window.scrollY;
    
    if (currentScroll > 50) {
        nav.classList.add('scrolled');
    } else {
        nav.classList.remove('scrolled');
    }
    
    lastScroll = currentScroll;
});

// Mobile menu toggle
if (navToggle && mobileMenu) {
    navToggle.addEventListener('click', () => {
        navToggle.classList.toggle('active');
        mobileMenu.classList.toggle('active');
    });
}

// ============================================
// PARALLAX EFFECT
// ============================================
const parallaxElements = document.querySelectorAll('[data-parallax]');
const hero = document.getElementById('hero');

function updateScrollParallax() {
    const scrollY = window.scrollY;
    const vh = window.innerHeight;
    
    parallaxElements.forEach(el => {
        const speed = parseFloat(el.dataset.parallax);
        const rect = el.getBoundingClientRect();
        
        // Only animate if element is visible
        if (rect.top < vh && rect.bottom > 0) {
            const relativeScroll = (vh - rect.top) / (vh + rect.height);
            const translateY = (relativeScroll - 0.5) * 100 * speed;
            
            // Handle existing transforms (if any)
            const currentTransform = el.style.transform;
            if (currentTransform.includes('translate3d')) {
                el.style.transform = currentTransform.replace(/translate3d\([^)]+\)/, `translate3d(0, ${translateY}px, 0)`);
            } else {
                el.style.transform = `translate3d(0, ${translateY}px, 0)`;
            }
        }
    });
}

// Mouse Parallax for Hero
if (hero) {
    hero.addEventListener('mousemove', (e) => {
        const { clientX, clientY } = e;
        const centerX = window.innerWidth / 2;
        const centerY = window.innerHeight / 2;
        
        const moveX = (clientX - centerX) / centerX;
        const moveY = (clientY - centerY) / centerY;
        
        const floatingElements = hero.querySelectorAll('.parallax-layer, .hero-content, .hero-code');
        
        floatingElements.forEach(el => {
            const speed = parseFloat(el.dataset.parallax) || 0.1;
            const x = moveX * 20 * speed;
            const y = moveY * 20 * speed;
            
            // Combine with scroll parallax by using translate3d
            // For simplicity, we just set it here, scroll will override or we merge
            // Merging is better:
            el.style.setProperty('--mx', `${x}px`);
            el.style.setProperty('--my', `${y}px`);
        });
    });
}

window.addEventListener('scroll', () => {
    requestAnimationFrame(updateScrollParallax);
});

// Update initial position
updateScrollParallax();

// ============================================
// ANIMATED COUNTERS
// ============================================
function animateCounter(element, target, duration = 2000) {
    const start = 0;
    const increment = target / (duration / 16);
    let current = start;
    
    const timer = setInterval(() => {
        current += increment;
        if (current >= target) {
            current = target;
            clearInterval(timer);
        }
        
        if (target >= 1) {
            element.textContent = Math.floor(current).toLocaleString();
        } else {
            element.textContent = current.toFixed(3);
        }
    }, 16);
}

// Intersection Observer for counters
const counterObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            const target = parseFloat(entry.target.dataset.count);
            animateCounter(entry.target, target);
            counterObserver.unobserve(entry.target);
        }
    });
}, { threshold: 0.5 });

document.querySelectorAll('[data-count]').forEach(el => {
    counterObserver.observe(el);
});

// ============================================
// COPY TO CLIPBOARD
// ============================================
document.querySelectorAll('.copy-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
        const text = btn.dataset.copy;
        try {
            await navigator.clipboard.writeText(text);
            btn.classList.add('copied');
            
            // Show feedback
            const originalHTML = btn.innerHTML;
            btn.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"></polyline></svg>`;
            
            setTimeout(() => {
                btn.classList.remove('copied');
                btn.innerHTML = originalHTML;
            }, 2000);
        } catch (err) {
            console.error('Failed to copy:', err);
        }
    });
});

// ============================================
// PARTICLES ANIMATION
// ============================================
const particlesContainer = document.getElementById('particles');

if (particlesContainer) {
    function createParticle() {
        const particle = document.createElement('div');
        particle.style.cssText = `
            position: absolute;
            width: ${Math.random() * 4 + 2}px;
            height: ${Math.random() * 4 + 2}px;
            background: rgba(255, 221, 0, ${Math.random() * 0.5 + 0.2});
            border-radius: 50%;
            left: ${Math.random() * 100}%;
            top: ${Math.random() * 100}%;
            pointer-events: none;
            animation: float ${Math.random() * 10 + 10}s linear infinite;
        `;
        particlesContainer.appendChild(particle);
        
        setTimeout(() => {
            particle.remove();
        }, 20000);
    }
    
    // Create initial particles
    for (let i = 0; i < 30; i++) {
        setTimeout(createParticle, i * 200);
    }
    
    // Continue creating particles
    setInterval(createParticle, 500);
    
    // Add float animation
    const style = document.createElement('style');
    style.textContent = `
        @keyframes float {
            0% {
                transform: translateY(0) translateX(0);
                opacity: 0;
            }
            10% {
                opacity: 1;
            }
            90% {
                opacity: 1;
            }
            100% {
                transform: translateY(-100vh) translateX(${Math.random() * 100 - 50}px);
                opacity: 0;
            }
        }
    `;
    document.head.appendChild(style);
}

// ============================================
// INTERSECTION OBSERVER FOR ANIMATIONS
// ============================================
const animateOnScroll = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.classList.add('animate-in');
        }
    });
}, { 
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
});

document.querySelectorAll('.feature-card, .mode-card, .stat-card, .evolution-card, .performance-card').forEach(el => {
    el.style.opacity = '0';
    el.style.transform = 'translateY(30px)';
    el.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
    animateOnScroll.observe(el);
});

// Add animate-in styles
const animateStyle = document.createElement('style');
animateStyle.textContent = `
    .animate-in {
        opacity: 1 !important;
        transform: translateY(0) !important;
    }
`;
document.head.appendChild(animateStyle);

// ============================================
// DOCS SIDEBAR (for docs page)
// ============================================
const sidebarToggle = document.querySelector('.sidebar-toggle');
const docsSidebar = document.querySelector('.docs-sidebar');

if (sidebarToggle && docsSidebar) {
    sidebarToggle.addEventListener('click', () => {
        docsSidebar.classList.toggle('active');
    });
}

// Active section highlighting
const sidebarLinks = document.querySelectorAll('.sidebar-link');
const sections = document.querySelectorAll('.docs-content h2[id], .docs-content h3[id]');

if (sidebarLinks.length > 0 && sections.length > 0) {
    const sectionObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const id = entry.target.id;
                sidebarLinks.forEach(link => {
                    link.classList.remove('active');
                    if (link.getAttribute('href') === `#${id}`) {
                        link.classList.add('active');
                    }
                });
            }
        });
    }, { 
        threshold: 0,
        rootMargin: '-80px 0px -70% 0px'
    });
    
    sections.forEach(section => {
        sectionObserver.observe(section);
    });
}

// ============================================
// PERFORMANCE BAR ANIMATIONS (for benchmarks page)
// ============================================
const performanceBars = document.querySelectorAll('.performance-bar-fill');

const barObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            const width = entry.target.dataset.width;
            setTimeout(() => {
                entry.target.style.width = width;
            }, 200);
            barObserver.unobserve(entry.target);
        }
    });
}, { threshold: 0.5 });

performanceBars.forEach(bar => {
    bar.style.width = '0%';
    barObserver.observe(bar);
});

// ============================================
// SMOOTH SCROLL FOR ANCHOR LINKS
// ============================================
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
        const href = this.getAttribute('href');
        if (href === '#') return;
        
        const target = document.querySelector(href);
        if (target) {
            e.preventDefault();
            lenis.scrollTo(target, {
                offset: -80
            });
        }
    });
});

// ============================================
// CODE BLOCK ENHANCEMENTS
// ============================================
document.querySelectorAll('pre code').forEach(block => {
    // Add line numbers (optional)
    const lines = block.innerHTML.split('\n');
    if (lines.length > 3) {
        const wrapper = document.createElement('div');
        wrapper.className = 'code-with-lines';
        wrapper.innerHTML = lines.map((line, i) => 
            `<div class="code-line"><span class="line-number">${i + 1}</span>${line}</div>`
        ).join('');
        // block.parentNode.replaceChild(wrapper, block);
    }
});

console.log('🚀 jsondb-high docs loaded successfully');
