// ============================================
// JSONDB-HIGH — Enhanced Professional Landing Page Script
// With Parallax, Advanced Animations, and Micro-interactions
// ============================================

// Initialize Lenis smooth scrolling
const isDocsPage = window.location.pathname.includes('docs.html');
let lenis = null;

if (!isDocsPage) {
    lenis = new Lenis({
        duration: 1.4,
        easing: (t) => Math.min(1, 1.001 - Math.pow(2, -10 * t)),
        orientation: 'vertical',
        gestureOrientation: 'vertical',
        smoothWheel: true,
        wheelMultiplier: 0.8,
        touchMultiplier: 1.5,
    });

    function raf(time) {
        lenis.raf(time);
        requestAnimationFrame(raf);
    }
    requestAnimationFrame(raf);
}

// ============================================
// NAVIGATION with Blur on Scroll
// ============================================
const nav = document.getElementById('nav');
let lastScroll = 0;
let ticking = false;

window.addEventListener('scroll', () => {
    if (!ticking) {
        requestAnimationFrame(() => {
            const currentScroll = window.scrollY;
            
            if (currentScroll > 50) {
                nav.classList.add('scrolled');
            } else {
                nav.classList.remove('scrolled');
            }
            
            // Hide/show nav based on scroll direction (optional)
            if (currentScroll > lastScroll && currentScroll > 200) {
                nav.style.transform = 'translateY(-100%)';
            } else {
                nav.style.transform = 'translateY(0)';
            }
            
            lastScroll = currentScroll;
            ticking = false;
        });
        ticking = true;
    }
}, { passive: true });

// Ensure nav is always visible on hover near top
document.addEventListener('mousemove', (e) => {
    if (e.clientY < 100) {
        nav.style.transform = 'translateY(0)';
    }
});

// Mobile menu
const navToggle = document.getElementById('navToggle');
const mobileMenu = document.getElementById('mobileMenu');

if (navToggle && mobileMenu) {
    navToggle.addEventListener('click', () => {
        navToggle.classList.toggle('active');
        mobileMenu.classList.toggle('active');
        document.body.style.overflow = mobileMenu.classList.contains('active') ? 'hidden' : '';
    });
}

// ============================================
// ADVANCED PARALLAX EFFECTS
// ============================================
function initParallax() {
    const parallaxElements = document.querySelectorAll('[data-parallax]');
    const heroGlow = document.querySelector('.hero-glow');
    const heroGrid = document.querySelector('.hero-grid');
    
    function updateParallax() {
        const scrollY = window.scrollY;
        const vh = window.innerHeight;
        
        parallaxElements.forEach(el => {
            const speed = parseFloat(el.dataset.parallax);
            const rect = el.getBoundingClientRect();
            
            if (rect.top < vh && rect.bottom > 0) {
                const progress = (vh - rect.top) / (vh + rect.height);
                const yPos = (progress - 0.5) * 100 * speed;
                el.style.transform = `translate3d(0, ${yPos}px, 0)`;
            }
        });
        
        // Parallax for hero elements
        if (heroGlow) {
            heroGlow.style.transform = `translateX(-50%) translateY(${scrollY * 0.3}px) scale(${1 + scrollY * 0.0003})`;
        }
        if (heroGrid) {
            heroGrid.style.transform = `translateY(${scrollY * 0.1}px)`;
        }
    }
    
    let parallaxTicking = false;
    window.addEventListener('scroll', () => {
        if (!parallaxTicking) {
            requestAnimationFrame(() => {
                updateParallax();
                parallaxTicking = false;
            });
            parallaxTicking = true;
        }
    }, { passive: true });
    
    // Initial call
    updateParallax();
}

initParallax();

// ============================================
// MOUSE PARALLAX FOR CARDS (3D Effect)
// ============================================
function init3DCards() {
    const cards = document.querySelectorAll('.performance-card, .evolution-card, .feature-card');
    
    cards.forEach(card => {
        card.addEventListener('mousemove', (e) => {
            const rect = card.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            
            const centerX = rect.width / 2;
            const centerY = rect.height / 2;
            
            const rotateX = (y - centerY) / 20;
            const rotateY = (centerX - x) / 20;
            
            card.style.transform = `perspective(1000px) rotateX(${rotateX}deg) rotateY(${rotateY}deg) translateZ(10px)`;
        });
        
        card.addEventListener('mouseleave', () => {
            card.style.transform = 'perspective(1000px) rotateX(0) rotateY(0) translateZ(0)';
        });
    });
}

init3DCards();

// ============================================
// ANIMATED COUNTERS with Easing
// ============================================
function animateCounter(element, target, duration = 2500) {
    const start = performance.now();
    const startValue = 0;
    
    function update(currentTime) {
        const elapsed = currentTime - start;
        const progress = Math.min(elapsed / duration, 1);
        
        // Ease out exponential
        const easeOut = 1 - Math.pow(1 - progress, 4);
        const current = startValue + (target - startValue) * easeOut;
        
        if (target < 1) {
            element.textContent = current.toFixed(target < 0.1 ? 4 : 1);
        } else if (target >= 1000000) {
            element.textContent = (current / 1000000).toFixed(2) + 'M';
        } else if (target >= 1000) {
            element.textContent = Math.floor(current).toLocaleString();
        } else {
            element.textContent = Math.floor(current).toLocaleString();
        }
        
        if (progress < 1) {
            requestAnimationFrame(update);
        } else {
            if (target >= 1000000) {
                element.textContent = (target / 1000000).toFixed(2) + 'M';
            } else {
                element.textContent = target < 1 ? target.toString() : Math.floor(target).toLocaleString();
            }
        }
    }
    
    requestAnimationFrame(update);
}

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
// COPY TO CLIPBOARD with Animation
// ============================================
document.querySelectorAll('.copy-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
        const text = btn.dataset.copy;
        try {
            await navigator.clipboard.writeText(text);
            
            const originalHTML = btn.innerHTML;
            btn.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>`;
            btn.classList.add('copied');
            
            // Add ripple effect
            const ripple = document.createElement('span');
            ripple.className = 'copy-ripple';
            btn.appendChild(ripple);
            
            setTimeout(() => {
                btn.innerHTML = originalHTML;
                btn.classList.remove('copied');
                ripple.remove();
            }, 2000);
        } catch (err) {
            console.error('Failed to copy:', err);
        }
    });
});

// ============================================
// SCROLL REVEAL ANIMATIONS
// ============================================
function initScrollReveal() {
    const revealElements = document.querySelectorAll(
        '.feature-card, .mode-item, .evolution-card, .performance-card, ' +
        '.use-case-card, .insight-card, .benchmark-section > h2, .benchmark-section > h3, ' +
        '.benchmark-section > p, .comparison-table-wrapper, .scroll-animate'
    );
    
    const revealObserver = new IntersectionObserver((entries) => {
        entries.forEach((entry, index) => {
            if (entry.isIntersecting) {
                // Add staggered delay based on element's position in the viewport
                const siblings = Array.from(entry.target.parentElement?.children || []);
                const siblingIndex = siblings.indexOf(entry.target);
                const delay = siblingIndex * 50;
                
                setTimeout(() => {
                    entry.target.style.opacity = '1';
                    entry.target.style.transform = 'translateY(0)';
                    entry.target.classList.add('visible');
                }, delay);
                
                revealObserver.unobserve(entry.target);
            }
        });
    }, { 
        threshold: 0.1,
        rootMargin: '0px 0px -80px 0px'
    });
    
    revealElements.forEach(el => {
        if (!el.classList.contains('visible')) {
            el.style.opacity = '0';
            el.style.transform = 'translateY(30px)';
            el.style.transition = 'opacity 0.7s cubic-bezier(0.4, 0, 0.2, 1), transform 0.7s cubic-bezier(0.4, 0, 0.2, 1)';
            revealObserver.observe(el);
        }
    });
}

// Initialize after DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initScrollReveal);
} else {
    initScrollReveal();
}

// ============================================
// ARCHITECTURE DIAGRAM HOVER
// ============================================
const archLayers = document.querySelectorAll('.arch-layer');

archLayers.forEach((layer, index) => {
    layer.addEventListener('mouseenter', () => {
        // Highlight current and dim others
        archLayers.forEach((l, i) => {
            if (i === index) {
                l.style.opacity = '1';
                l.style.transform = 'scale(1.02)';
                l.querySelector('.layer-box').style.borderColor = 'var(--color-primary)';
            } else {
                l.style.opacity = '0.4';
                l.style.transform = 'scale(0.98)';
                l.querySelector('.layer-box').style.borderColor = 'var(--color-border)';
            }
        });
    });
});

const archDiagram = document.querySelector('.arch-diagram');
if (archDiagram) {
    archDiagram.addEventListener('mouseleave', () => {
        archLayers.forEach(layer => {
            layer.style.opacity = '1';
            layer.style.transform = 'scale(1)';
            const box = layer.querySelector('.layer-box');
            if (box) {
                box.style.borderColor = layer.classList.contains('highlight') 
                    ? 'rgba(245, 158, 11, 0.4)' 
                    : 'var(--color-border)';
            }
        });
    });
}

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
            
            // Close mobile menu if open
            if (mobileMenu && mobileMenu.classList.contains('active')) {
                navToggle.classList.remove('active');
                mobileMenu.classList.remove('active');
                document.body.style.overflow = '';
            }
            
            if (lenis) {
                lenis.scrollTo(target, { offset: -100, duration: 1.2 });
            } else {
                const offsetTop = target.getBoundingClientRect().top + window.scrollY - 100;
                window.scrollTo({
                    top: offsetTop,
                    behavior: 'smooth'
                });
            }
        }
    });
});

// ============================================
// DOCS SIDEBAR (with improved highlighting)
// ============================================
const sidebarToggle = document.querySelector('.sidebar-toggle');
const docsSidebar = document.querySelector('.docs-sidebar');

if (sidebarToggle && docsSidebar) {
    sidebarToggle.addEventListener('click', () => {
        docsSidebar.classList.toggle('active');
        sidebarToggle.classList.toggle('active');
    });
    
    // Close sidebar when clicking outside on mobile
    document.addEventListener('click', (e) => {
        if (window.innerWidth <= 968 && 
            !docsSidebar.contains(e.target) && 
            !sidebarToggle.contains(e.target)) {
            docsSidebar.classList.remove('active');
            sidebarToggle.classList.remove('active');
        }
    });
}

// Active section highlighting with better algorithm
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
                        
                        // Scroll the sidebar to show active link
                        if (docsSidebar && window.innerWidth > 968) {
                            const linkRect = link.getBoundingClientRect();
                            const sidebarRect = docsSidebar.getBoundingClientRect();
                            
                            if (linkRect.top < sidebarRect.top + 100 || linkRect.bottom > sidebarRect.bottom - 100) {
                                link.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            }
                        }
                    }
                });
            }
        });
    }, { 
        threshold: 0,
        rootMargin: '-100px 0px -60% 0px'
    });
    
    sections.forEach(section => {
        sectionObserver.observe(section);
    });
}

// ============================================
// PERFORMANCE BAR ANIMATIONS
// ============================================
const performanceBars = document.querySelectorAll('.performance-bar-fill');

const barObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            const width = entry.target.dataset.width;
            setTimeout(() => {
                entry.target.style.width = width;
            }, 300);
            barObserver.unobserve(entry.target);
        }
    });
}, { threshold: 0.3 });

performanceBars.forEach(bar => {
    bar.style.width = '0%';
    bar.style.transition = 'width 1.5s cubic-bezier(0.34, 1.56, 0.64, 1)';
    barObserver.observe(bar);
});

// ============================================
// TYPING ANIMATION FOR CODE BLOCKS
// ============================================
function initTypingEffect() {
    const codeBlocks = document.querySelectorAll('.code-content');
    
    codeBlocks.forEach(block => {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    block.classList.add('typing-active');
                    observer.unobserve(entry.target);
                }
            });
        }, { threshold: 0.5 });
        
        observer.observe(block);
    });
}

initTypingEffect();

// ============================================
// MAGNETIC BUTTONS (subtle effect)
// ============================================
function initMagneticButtons() {
    const buttons = document.querySelectorAll('.btn-primary, .btn-secondary');
    
    buttons.forEach(btn => {
        btn.addEventListener('mousemove', (e) => {
            const rect = btn.getBoundingClientRect();
            const x = e.clientX - rect.left - rect.width / 2;
            const y = e.clientY - rect.top - rect.height / 2;
            
            btn.style.transform = `translate(${x * 0.1}px, ${y * 0.1}px)`;
        });
        
        btn.addEventListener('mouseleave', () => {
            btn.style.transform = 'translate(0, 0)';
        });
    });
}

initMagneticButtons();

// ============================================
// CURSOR GLOW EFFECT
// ============================================
function initCursorGlow() {
    const cursorGlow = document.createElement('div');
    cursorGlow.className = 'cursor-glow';
    cursorGlow.style.cssText = `
        position: fixed;
        width: 400px;
        height: 400px;
        background: radial-gradient(circle, rgba(245, 158, 11, 0.08) 0%, transparent 70%);
        pointer-events: none;
        z-index: -1;
        transform: translate(-50%, -50%);
        transition: opacity 0.3s;
        filter: blur(40px);
    `;
    document.body.appendChild(cursorGlow);
    
    let cursorX = 0, cursorY = 0;
    let glowX = 0, glowY = 0;
    
    document.addEventListener('mousemove', (e) => {
        cursorX = e.clientX;
        cursorY = e.clientY;
    });
    
    function animateCursor() {
        glowX += (cursorX - glowX) * 0.1;
        glowY += (cursorY - glowY) * 0.1;
        
        cursorGlow.style.left = glowX + 'px';
        cursorGlow.style.top = glowY + 'px';
        
        requestAnimationFrame(animateCursor);
    }
    
    animateCursor();
    
    // Hide on mobile
    if (window.matchMedia('(pointer: coarse)').matches) {
        cursorGlow.style.display = 'none';
    }
}

initCursorGlow();

// ============================================
// NUMBER TICKER FOR PERF VALUES
// ============================================
function initNumberTickers() {
    const perfValues = document.querySelectorAll('.perf-stat-value');
    
    const tickerObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const text = entry.target.textContent;
                const num = parseFloat(text.replace(/,/g, ''));
                
                if (!isNaN(num)) {
                    animateCounter(entry.target, num, 2000);
                }
                
                tickerObserver.unobserve(entry.target);
            }
        });
    }, { threshold: 0.5 });
    
    perfValues.forEach(val => {
        tickerObserver.observe(val);
    });
}

initNumberTickers();

// ============================================
// LAZY LOAD IMAGES WITH FADE
// ============================================
if ('IntersectionObserver' in window) {
    const imageObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const img = entry.target;
                if (img.dataset.src) {
                    img.src = img.dataset.src;
                    img.onload = () => {
                        img.classList.add('loaded');
                    };
                }
                imageObserver.unobserve(img);
            }
        });
    });
    
    document.querySelectorAll('img[data-src]').forEach(img => {
        imageObserver.observe(img);
    });
}

// ============================================
// KEYBOARD SHORTCUTS
// ============================================
document.addEventListener('keydown', (e) => {
    // Press '/' to focus search (if exists)
    if (e.key === '/' && !e.ctrlKey && !e.metaKey) {
        const searchInput = document.querySelector('.search-input');
        if (searchInput && document.activeElement !== searchInput) {
            e.preventDefault();
            searchInput.focus();
        }
    }
    
    // Press 'Escape' to close mobile menu
    if (e.key === 'Escape') {
        if (mobileMenu && mobileMenu.classList.contains('active')) {
            navToggle.classList.remove('active');
            mobileMenu.classList.remove('active');
            document.body.style.overflow = '';
        }
        if (docsSidebar && docsSidebar.classList.contains('active')) {
            docsSidebar.classList.remove('active');
            sidebarToggle.classList.remove('active');
        }
    }
});

// ============================================
// PREFERS REDUCED MOTION
// ============================================
const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

if (prefersReducedMotion.matches) {
    // Disable animations for users who prefer reduced motion
    document.documentElement.style.setProperty('--duration-fast', '0s');
    document.documentElement.style.setProperty('--duration-normal', '0s');
    document.documentElement.style.setProperty('--duration-slow', '0s');
    
    // Remove cursor glow
    const cursorGlow = document.querySelector('.cursor-glow');
    if (cursorGlow) cursorGlow.remove();
}

// ============================================
// INITIALIZATION COMPLETE
// ============================================
console.log('%c⚡ jsondb-high docs loaded', 'color: #f59e0b; font-weight: bold; font-size: 14px;');
console.log('%cExperience the speed of Rust-powered JSON database', 'color: #a1a1aa; font-size: 12px;');
