// Initialize Mermaid
mermaid.initialize({
  startOnLoad: true,
  theme: 'dark',
  themeVariables: {
    darkMode: true,
    primaryColor: '#4facfe',
    primaryTextColor: '#f8fafc',
    primaryBorderColor: '#334155',
    lineColor: '#64748b',
    secondaryColor: '#7c3aed',
    tertiaryColor: '#f59e0b',
    background: 'transparent',
    mainBkg: 'rgba(30, 41, 59, 0.6)',
    secondBkg: 'rgba(51, 65, 85, 0.5)',
    textColor: '#f8fafc',
    fontSize: '14px',
    nodeBorder: '#4facfe',
    clusterBkg: 'rgba(30, 41, 59, 0.4)',
    clusterBorder: '#334155',
    titleColor: '#f8fafc',
    edgeLabelBackground: 'transparent',
    noteBkgColor: 'rgba(30, 41, 59, 0.6)',
    noteTextColor: '#f8fafc',
    actorBkg: 'rgba(30, 41, 59, 0.8)',
    actorBorder: '#4facfe',
    actorTextColor: '#f8fafc',
    actorLineColor: '#64748b',
    signalColor: '#f8fafc',
    signalTextColor: '#f8fafc',
    labelBoxBkgColor: 'rgba(30, 41, 59, 0.6)',
    labelBoxBorderColor: '#334155',
    labelTextColor: '#f8fafc',
    loopTextColor: '#f8fafc',
    activationBorderColor: '#4facfe',
    activationBkgColor: 'rgba(79, 172, 254, 0.15)',
    sequenceNumberColor: '#f8fafc',
  },
});

// Header scroll effect and progress bar
let ticking = false;
window.addEventListener('scroll', function () {
  const header = document.querySelector('.header');
  if (window.scrollY > 50) {
    header.classList.add('scrolled');
  } else {
    header.classList.remove('scrolled');
  }

  if (!ticking) {
    requestAnimationFrame(() => {
      updateScrollProgress();
      ticking = false;
    });
    ticking = true;
  }
});

function updateScrollProgress() {
  const progressBar = document.querySelector('.scroll-progress');
  if (!progressBar) return;

  const windowHeight = window.innerHeight;
  const documentHeight = document.documentElement.scrollHeight;
  const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
  const scrollPercentage = scrollTop / (documentHeight - windowHeight);

  progressBar.style.transform = 'scaleX(' + Math.min(scrollPercentage, 1) + ')';
}

// Back to top button
const backToTopButton = document.querySelector('.back-to-top');

window.addEventListener('scroll', function () {
  if (window.scrollY > 300) {
    backToTopButton.classList.add('visible');
  } else {
    backToTopButton.classList.remove('visible');
  }
});

// Smooth scroll for navigation links
document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
  anchor.addEventListener('click', function (e) {
    e.preventDefault();
    const target = document.querySelector(this.getAttribute('href'));
    if (target) {
      const headerOffset = 80;
      const elementPosition = target.getBoundingClientRect().top;
      const offsetPosition = elementPosition + window.pageYOffset - headerOffset;

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth',
      });
    }
  });
});

// Copy code functionality
function copyCode(button) {
  const codeBlock = button.closest('.code-block').querySelector('code');
  const code = codeBlock.textContent;

  navigator.clipboard
    .writeText(code)
    .then(() => {
      const originalText = button.textContent;
      button.textContent = '✓ Copied!';
      button.style.background = 'rgba(16, 185, 129, 0.3)';

      setTimeout(() => {
        button.textContent = originalText;
        button.style.background = '';
      }, 2000);
    })
    .catch((err) => {
      console.error('Failed to copy:', err);
      button.textContent = '✗ Failed';
      setTimeout(() => {
        button.textContent = '📋 Copy';
      }, 2000);
    });
}

// Animate elements on scroll — add 'reveal' class so they start hidden
const observerOptions = {
  threshold: 0.1,
  rootMargin: '0px 0px -50px 0px',
};

const observer = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      entry.target.classList.add('fade-in');
      observer.unobserve(entry.target);
    }
  });
}, observerOptions);

// Mark and observe all sections
document.querySelectorAll('.section').forEach((section) => {
  section.classList.add('reveal');
  observer.observe(section);
});

// Mark and observe all cards
document.querySelectorAll('.feature-card, .deployment-card, .use-case-card, .tech-category, .resources-box').forEach((card) => {
  card.classList.add('reveal');
  observer.observe(card);
});

// Mark and observe resource links individually with stagger
document.querySelectorAll('.resource-link').forEach((link, i) => {
  link.classList.add('reveal');
  link.style.animationDelay = (i * 0.07) + 's';
  observer.observe(link);
});

// Mark and observe stat items — counter starts only after reveal finishes
document.querySelectorAll('.stat-item').forEach((stat) => {
  stat.classList.add('reveal');
});

function animateCounter(element, target, duration = 2000) {
  let start = 0;
  const increment = target / (duration / 16);
  const timer = setInterval(() => {
    start += increment;
    if (start >= target) {
      element.textContent = target + (element.dataset.suffix || '');
      clearInterval(timer);
    } else {
      element.textContent = Math.floor(start) + (element.dataset.suffix || '');
    }
  }, 16);
}

const statsObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting && !entry.target.classList.contains('counted')) {
        entry.target.classList.add('counted');
        entry.target.classList.add('fade-in');
        // Wait for the 0.6s fade-in animation to finish, then count up
        setTimeout(() => {
          const statNumber = entry.target.querySelector('.stat-number');
          const target = parseInt(statNumber.dataset.count);
          animateCounter(statNumber, target);
        }, 600);
        statsObserver.unobserve(entry.target);
      }
    });
  },
  { threshold: 0.5 },
);

document.querySelectorAll('.stat-item').forEach((stat) => {
  statsObserver.observe(stat);
});

// Mobile menu toggle
const mobileToggle = document.querySelector('.mobile-menu-toggle');
const navMenu = document.querySelector('.nav-menu');

if (mobileToggle && navMenu) {
  const setMobileMenuState = (isOpen) => {
    navMenu.classList.toggle('open', isOpen);
    mobileToggle.classList.toggle('is-open', isOpen);
    mobileToggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    mobileToggle.setAttribute('aria-label', isOpen ? 'Close menu' : 'Open menu');
  };

  setMobileMenuState(navMenu.classList.contains('open'));

  mobileToggle.addEventListener('click', () => {
    setMobileMenuState(!navMenu.classList.contains('open'));
  });

  // Close menu when a link is clicked
  navMenu.querySelectorAll('a').forEach((link) => {
    link.addEventListener('click', () => {
      setMobileMenuState(false);
    });
  });
}

// Enhanced link tracking
document.querySelectorAll('a[href^="http"]').forEach((link) => {
  link.setAttribute('target', '_blank');
  link.setAttribute('rel', 'noopener noreferrer');
});

// Highlight active section in navigation
const sections = document.querySelectorAll('.section[id]');
const navLinks = document.querySelectorAll('.nav-link');
let currentActiveSection = '';

const updateActiveNavLink = () => {
  if (!sections.length || !navLinks.length) {
    return;
  }

  const viewportMidpoint = window.scrollY + window.innerHeight * 0.4;
  let nextActiveSection = sections[0].getAttribute('id');

  sections.forEach((section) => {
    if (viewportMidpoint >= section.offsetTop) {
      nextActiveSection = section.getAttribute('id');
    }
  });

  if (nextActiveSection === currentActiveSection) {
    return;
  }

  currentActiveSection = nextActiveSection;
  navLinks.forEach((link) => {
    link.classList.toggle('active', link.getAttribute('href').substring(1) === currentActiveSection);
  });
};

let navHighlightTicking = false;
window.addEventListener(
  'scroll',
  () => {
    if (navHighlightTicking) {
      return;
    }
    navHighlightTicking = true;
    requestAnimationFrame(() => {
      updateActiveNavLink();
      navHighlightTicking = false;
    });
  },
  { passive: true },
);

window.addEventListener('resize', updateActiveNavLink);
updateActiveNavLink();

// Add ripple effect to buttons
document.querySelectorAll('.btn-primary, .btn-secondary, .cta-button').forEach((button) => {
  button.addEventListener('click', function (e) {
    const ripple = document.createElement('span');
    const rect = this.getBoundingClientRect();
    const size = Math.max(rect.width, rect.height);
    const x = e.clientX - rect.left - size / 2;
    const y = e.clientY - rect.top - size / 2;

    ripple.style.cssText = `
            width: ${size}px;
            height: ${size}px;
            left: ${x}px;
            top: ${y}px;
            position: absolute;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.3);
            transform: scale(0);
            animation: ripple 0.6s ease-out;
            pointer-events: none;
        `;

    this.style.position = 'relative';
    this.style.overflow = 'hidden';
    this.appendChild(ripple);

    setTimeout(() => ripple.remove(), 600);
  });
});

// Add CSS for ripple animation
const style = document.createElement('style');
style.textContent = `
    @keyframes ripple {
        to {
            transform: scale(4);
            opacity: 0;
        }
    }
    
    .active {
        color: var(--primary-color) !important;
    }
    
    .active::after {
        width: 100% !important;
    }
`;
document.head.appendChild(style);

// Performance optimization: Lazy load diagrams
const diagramObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        const diagram = entry.target;
        if (diagram.classList.contains('mermaid') && !diagram.dataset.processed) {
          mermaid.init(undefined, diagram);
          diagram.dataset.processed = 'true';
        }
      }
    });
  },
  { rootMargin: '100px' },
);

document.querySelectorAll('.mermaid').forEach((diagram) => {
  diagramObserver.observe(diagram);
});

// Easter egg: Konami code
let konamiCode = [];
const konamiPattern = [
  'ArrowUp',
  'ArrowUp',
  'ArrowDown',
  'ArrowDown',
  'ArrowLeft',
  'ArrowRight',
  'ArrowLeft',
  'ArrowRight',
  'b',
  'a',
];

document.addEventListener('keydown', (e) => {
  konamiCode.push(e.key);
  konamiCode = konamiCode.slice(-10);

  if (konamiCode.join(',') === konamiPattern.join(',')) {
    document.body.style.animation = 'rainbow 2s infinite';
    setTimeout(() => {
      document.body.style.animation = '';
    }, 5000);
  }
});

const rainbowStyle = document.createElement('style');
rainbowStyle.textContent = `
    @keyframes rainbow {
        0% { filter: hue-rotate(0deg); }
        100% { filter: hue-rotate(360deg); }
    }
`;
document.head.appendChild(rainbowStyle);

// Analytics and tracking (placeholder)
function trackEvent(category, action, label) {
  if (typeof gtag !== 'undefined') {
    gtag('event', action, {
      event_category: category,
      event_label: label,
    });
  }
  console.log(`Event tracked: ${category} - ${action} - ${label}`);
}

// Track CTA clicks
document.querySelectorAll('.cta-button, .btn-primary').forEach((button) => {
  button.addEventListener('click', () => {
    trackEvent('CTA', 'Click', button.textContent);
  });
});

// Log page load time
window.addEventListener('load', () => {
  const loadTime = performance.timing.loadEventEnd - performance.timing.navigationStart;
  console.log(`Page loaded in ${loadTime}ms`);
  trackEvent('Performance', 'PageLoad', `${loadTime}ms`);
});

// Console message
console.log('%c🚀 End-to-End Data Pipeline Wiki', 'font-size: 20px; font-weight: bold; color: #4facfe;');
console.log('%cBuilt with ❤️ for data engineers and scientists', 'font-size: 12px; color: #cbd5e1;');
console.log('%cGitHub: https://github.com/hoangsonww/End-to-End-Data-Pipeline', 'font-size: 12px; color: #7c3aed;');
