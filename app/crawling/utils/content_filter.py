"""
Content filter for skipping unwanted pages.
"""

import re
from dataclasses import dataclass, field
from typing import Optional, Set


@dataclass
class ContentFilter:
    """
    Filter for skipping unwanted pages during crawling.
    
    Features:
    - Skip 404 and error pages
    - Skip login/auth pages
    - Skip duplicate content
    - URL pattern matching (include/exclude)
    
    Usage:
        filter = ContentFilter(skip_login_pages=True)
        
        should_skip, reason = filter.should_skip_url(url)
        if should_skip:
            print(f"Skipping: {reason}")
    """
    
    skip_404: bool = True
    skip_login_pages: bool = True
    skip_duplicates: bool = True
    
    include_patterns: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    
    # Internal state
    _seen_hashes: Set[str] = field(default_factory=set, repr=False)
    _compiled_includes: list[re.Pattern] = field(default_factory=list, repr=False)
    _compiled_excludes: list[re.Pattern] = field(default_factory=list, repr=False)
    
    # Known login page patterns
    LOGIN_PATTERNS = [
        r"/login",
        r"/signin",
        r"/sign-in",
        r"/auth",
        r"/oauth",
        r"/sso",
        r"/account/login",
        r"/user/login",
        r"[?&]next=",
        r"[?&]redirect=",
        r"[?&]return_to=",
    ]
    
    def __post_init__(self):
        """Compile regex patterns."""
        self._compile_patterns()
    
    def _compile_patterns(self) -> None:
        """Compile include/exclude patterns to regex."""
        self._compiled_includes = [
            re.compile(p, re.IGNORECASE) for p in self.include_patterns
        ]
        self._compiled_excludes = [
            re.compile(p, re.IGNORECASE) for p in self.exclude_patterns
        ]
        self._login_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.LOGIN_PATTERNS
        ]
    
    def should_skip_url(self, url: str) -> tuple[bool, Optional[str]]:
        """
        Check if URL should be skipped.
        
        Args:
            url: URL to check
            
        Returns:
            Tuple of (should_skip, reason)
        """
        # Check exclude patterns first
        for pattern in self._compiled_excludes:
            if pattern.search(url):
                return True, f"Excluded by pattern: {pattern.pattern}"
        
        # Check include patterns (if any are set, URL must match at least one)
        if self._compiled_includes:
            matched = any(p.search(url) for p in self._compiled_includes)
            if not matched:
                return True, "URL does not match any include pattern"
        
        # Check login patterns
        if self.skip_login_pages:
            for pattern in self._login_patterns:
                if pattern.search(url):
                    return True, "Login/auth page"
        
        return False, None
    
    def is_duplicate_content(self, content_hash: str) -> bool:
        """
        Check if content has been seen before.
        
        Args:
            content_hash: Hash of page content
            
        Returns:
            True if this content hash was already seen
        """
        if not self.skip_duplicates:
            return False
        
        if content_hash in self._seen_hashes:
            return True
        
        self._seen_hashes.add(content_hash)
        return False
    
    def is_error_page(self, status_code: int) -> bool:
        """
        Check if response is an error page.
        
        Args:
            status_code: HTTP status code
            
        Returns:
            True if this is an error page that should be skipped
        """
        if not self.skip_404:
            return False
        
        return status_code >= 400
    
    def reset(self) -> None:
        """Reset internal state (seen hashes)."""
        self._seen_hashes.clear()
