// Auto-submit form when filters change
document.addEventListener('DOMContentLoaded', function() {
    // Theme toggle
    const body = document.body;
    const toggleBtn = document.getElementById('themeToggle');
    const iconEl = document.getElementById('themeIcon');
    const sunSrc = iconEl ? iconEl.dataset.sun : null;
    const moonSrc = iconEl ? iconEl.dataset.moon : null;
    // Icon-only now, no text label
    const storedTheme = localStorage.getItem('site-theme');
    if (storedTheme === 'light') {
        body.classList.add('light-theme');
        if (iconEl && moonSrc) { iconEl.src = moonSrc; iconEl.setAttribute('data-icon','moon'); }
    } else {
        // default dark theme -> show sun
        if (iconEl && sunSrc) { iconEl.src = sunSrc; iconEl.setAttribute('data-icon','sun'); }
    }
    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
            const isLight = body.classList.toggle('light-theme');
            if (isLight) {
                // Light theme active -> show moon icon
                localStorage.setItem('site-theme', 'light');
                if (iconEl && moonSrc) { iconEl.src = moonSrc; iconEl.setAttribute('data-icon','moon'); }
            } else {
                // Dark theme -> show sun
                localStorage.setItem('site-theme', 'dark');
                if (iconEl && sunSrc) { iconEl.src = sunSrc; iconEl.setAttribute('data-icon','sun'); }
            }
        });
    }
    const filterForm = document.querySelector('.filters-form');
    const filterSelects = document.querySelectorAll('.filter-select');
    
    if (filterForm && filterSelects.length > 0) {
        filterSelects.forEach(select => {
            select.addEventListener('change', function() {
                // Reset to page 1 when filters change
                const pageInput = document.querySelector('input[name="page"]');
                if (pageInput) {
                    pageInput.value = '1';
                } else {
                    const hiddenPageInput = document.createElement('input');
                    hiddenPageInput.type = 'hidden';
                    hiddenPageInput.name = 'page';
                    hiddenPageInput.value = '1';
                    filterForm.appendChild(hiddenPageInput);
                }
                
                filterForm.submit();
            });
        });
    }
    
    // Sortable table headers
    const sortableHeaders = document.querySelectorAll('.sortable');
    sortableHeaders.forEach(header => {
        header.addEventListener('click', function() {
            const column = this.getAttribute('data-column');
            const currentUrl = new URL(window.location);
            const currentSort = currentUrl.searchParams.get('sort');
            const currentOrder = currentUrl.searchParams.get('order');
            
            // Determine new sort order
            let newOrder = 'desc';
            if (currentSort === column && currentOrder === 'desc') {
                newOrder = 'asc';
            }
            
            // Update URL parameters
            currentUrl.searchParams.set('sort', column);
            currentUrl.searchParams.set('order', newOrder);
            currentUrl.searchParams.set('page', '1'); // Reset to page 1
            
            // Navigate to new URL
            window.location.href = currentUrl.toString();
        });
    });
    
    // Add loading state to buttons
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach(button => {
        button.addEventListener('click', function() {
            this.style.opacity = '0.7';
            this.style.pointerEvents = 'none';
            
            setTimeout(() => {
                this.style.opacity = '1';
                this.style.pointerEvents = 'auto';
            }, 2000);
        });
    });
    
    // Smooth scroll for anchor links
    const anchorLinks = document.querySelectorAll('a[href^="#"]');
    anchorLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth'
                });
            }
        });
    });
    
    // Add hover effects to table rows
    const tableRows = document.querySelectorAll('.player-row');
    tableRows.forEach(row => {
        row.addEventListener('mouseenter', function() {
            this.style.transform = 'scale(1.01)';
            this.style.transition = 'transform 0.2s ease';
        });
        
        row.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1)';
        });
    });
});
