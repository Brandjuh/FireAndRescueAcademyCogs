"""
Fuzzy Search Engine for FAQ System
Uses RapidFuzz for intelligent matching with synonyms and typo tolerance.
"""

from typing import List, Tuple, Optional
from rapidfuzz import fuzz, process
from .models import FAQItem, HelpshiftArticle, SearchResult, Source
from .synonyms import SynonymManager


class FuzzySearchEngine:
    """
    Advanced search engine with fuzzy matching, synonym expansion, and ranking.
    
    Ranking Formula:
        score = 0.55 * fuzzy(title, query*)
              + 0.30 * fuzzy(snippet, query*)
              + 0.10 * fuzzy(section, query*)
              + 0.05 * exact_boost(title, query)
    
    Where query* = query expanded with synonyms and typo variants
    """
    
    # Weights for ranking formula
    TITLE_WEIGHT = 0.55
    CONTENT_WEIGHT = 0.30
    CATEGORY_WEIGHT = 0.10
    EXACT_BOOST = 0.05
    
    # Score thresholds
    MIN_SCORE = 30  # Absolute minimum to consider
    SUGGESTION_THRESHOLD = 75  # Below this, show suggestions
    
    def __init__(self, synonym_manager: SynonymManager, suggestion_threshold: int = 75):
        """
        Initialize search engine.
        
        Args:
            synonym_manager: SynonymManager instance for query expansion
            suggestion_threshold: Score below which to show suggestions
        """
        self.synonym_manager = synonym_manager
        self.suggestion_threshold = suggestion_threshold
    
    def search_custom(
        self,
        query: str,
        faq_items: List[FAQItem],
        max_results: int = 5
    ) -> Tuple[Optional[SearchResult], List[SearchResult]]:
        """
        Search through custom FAQ items.
        
        Args:
            query: Search query
            faq_items: List of FAQItem objects to search
            max_results: Maximum number of results to return
            
        Returns:
            Tuple of (main_result, suggestions)
            - main_result: Top result if score >= threshold, else None
            - suggestions: List of alternative results
        """
        if not faq_items or not query.strip():
            return None, []
        
        # Expand query with synonyms
        expanded_queries = self.synonym_manager.expand_query(query)
        
        # Score each FAQ
        scored_results = []
        for item in faq_items:
            score = self._score_faq_item(query, expanded_queries, item)
            if score >= self.MIN_SCORE:
                result = SearchResult.from_faq_item(item, score)
                scored_results.append(result)
        
        # Sort by score
        scored_results.sort(key=lambda x: x.score, reverse=True)
        
        # Determine main result and suggestions
        if not scored_results:
            return None, []
        
        top_score = scored_results[0].score
        
        if top_score >= self.suggestion_threshold:
            # Strong match - return as main result with remaining as suggestions
            return scored_results[0], scored_results[1:max_results]
        else:
            # Weak match - return all as suggestions
            return None, scored_results[:max_results]
    
    def search_helpshift(
        self,
        query: str,
        articles: List[HelpshiftArticle],
        max_results: int = 5
    ) -> Tuple[Optional[SearchResult], List[SearchResult]]:
        """
        Search through Helpshift articles.
        
        Args:
            query: Search query
            articles: List of HelpshiftArticle objects
            max_results: Maximum number of results
            
        Returns:
            Tuple of (main_result, suggestions)
        """
        if not articles or not query.strip():
            return None, []
        
        expanded_queries = self.synonym_manager.expand_query(query)
        
        scored_results = []
        for article in articles:
            score = self._score_helpshift_article(query, expanded_queries, article)
            if score >= self.MIN_SCORE:
                result = SearchResult.from_helpshift_article(article, score)
                scored_results.append(result)
        
        scored_results.sort(key=lambda x: x.score, reverse=True)
        
        if not scored_results:
            return None, []
        
        top_score = scored_results[0].score
        
        if top_score >= self.suggestion_threshold:
            return scored_results[0], scored_results[1:max_results]
        else:
            return None, scored_results[:max_results]
    
    def search_combined(
        self,
        query: str,
        faq_items: List[FAQItem],
        helpshift_articles: List[HelpshiftArticle],
        max_results: int = 5,
        custom_boost: float = 5.0
    ) -> Tuple[Optional[SearchResult], List[SearchResult]]:
        """
        Search both custom and Helpshift sources, combining results.
        
        Args:
            query: Search query
            faq_items: Custom FAQs
            helpshift_articles: Helpshift articles
            max_results: Maximum total results
            custom_boost: Score boost for custom FAQs (default 5.0)
            
        Returns:
            Tuple of (main_result, suggestions)
        """
        expanded_queries = self.synonym_manager.expand_query(query)
        
        all_results = []
        
        # Score custom FAQs (with boost)
        for item in faq_items:
            score = self._score_faq_item(query, expanded_queries, item)
            if score >= self.MIN_SCORE:
                result = SearchResult.from_faq_item(item, score + custom_boost)
                all_results.append(result)
        
        # Score Helpshift articles
        for article in helpshift_articles:
            score = self._score_helpshift_article(query, expanded_queries, article)
            if score >= self.MIN_SCORE:
                result = SearchResult.from_helpshift_article(article, score)
                all_results.append(result)
        
        # Sort by adjusted score
        all_results.sort(key=lambda x: x.score, reverse=True)
        
        if not all_results:
            return None, []
        
        top_score = all_results[0].score - (custom_boost if all_results[0].source == Source.CUSTOM else 0)
        
        if top_score >= self.suggestion_threshold:
            return all_results[0], all_results[1:max_results]
        else:
            return None, all_results[:max_results]
    
    def _score_faq_item(
        self,
        query: str,
        expanded_queries: set,
        item: FAQItem
    ) -> float:
        """Score a custom FAQ item against the query."""
        query_lower = query.lower()
        
        # Title scoring (with synonym expansion)
        title_score = self._best_fuzzy_score(item.question.lower(), expanded_queries)
        
        # Content scoring
        content_excerpt = item.get_excerpt(300).lower()
        content_score = self._best_fuzzy_score(content_excerpt, expanded_queries)
        
        # Category scoring
        category_score = 0.0
        if item.category:
            category_score = self._best_fuzzy_score(item.category.lower(), expanded_queries)
        
        # Exact match boost
        exact_boost = 0.0
        if query_lower in item.question.lower():
            exact_boost = 100.0
        
        # Apply ranking formula
        total_score = (
            self.TITLE_WEIGHT * title_score +
            self.CONTENT_WEIGHT * content_score +
            self.CATEGORY_WEIGHT * category_score +
            self.EXACT_BOOST * exact_boost
        )
        
        return min(total_score, 100.0)
    
    def _score_helpshift_article(
        self,
        query: str,
        expanded_queries: set,
        article: HelpshiftArticle
    ) -> float:
        """Score a Helpshift article against the query."""
        query_lower = query.lower()
        
        # Title scoring
        title_score = self._best_fuzzy_score(article.title.lower(), expanded_queries)
        
        # Body scoring (use body_md instead of snippet)
        body_excerpt = article.get_excerpt(300)
        content_score = self._best_fuzzy_score(body_excerpt.lower(), expanded_queries)
        
        # Section scoring
        section_score = 0.0
        if article.section_name:
            section_score = self._best_fuzzy_score(article.section_name.lower(), expanded_queries)
        
        # Exact match boost
        exact_boost = 0.0
        if query_lower in article.title.lower():
            exact_boost = 100.0
        
        # Apply ranking formula
        total_score = (
            self.TITLE_WEIGHT * title_score +
            self.CONTENT_WEIGHT * content_score +
            self.CATEGORY_WEIGHT * section_score +
            self.EXACT_BOOST * exact_boost
        )
        
        return min(total_score, 100.0)
    
    def _best_fuzzy_score(self, text: str, queries: set) -> float:
        """
        Get the best fuzzy match score for text against multiple query variants.
        
        Args:
            text: Text to match against
            queries: Set of query variants (expanded with synonyms)
            
        Returns:
            Best match score (0-100)
        """
        if not text or not queries:
            return 0.0
        
        best_score = 0.0
        
        for query in queries:
            # Use token_set_ratio for better matching of multi-word queries
            score = fuzz.token_set_ratio(query, text)
            best_score = max(best_score, score)
            
            # Also try partial ratio for substring matches
            partial_score = fuzz.partial_ratio(query, text)
            best_score = max(best_score, partial_score * 0.9)  # Slight penalty for partial
        
        return best_score
    
    def autocomplete_search(
        self,
        partial_query: str,
        faq_items: List[FAQItem],
        helpshift_titles: List[str],
        max_results: int = 20
    ) -> List[Tuple[str, float]]:
        """
        Search for autocomplete suggestions.
        
        Args:
            partial_query: Partial user input
            faq_items: Custom FAQ items
            helpshift_titles: Recent Helpshift article titles
            max_results: Maximum suggestions to return
            
        Returns:
            List of (title, score) tuples sorted by relevance
        """
        if not partial_query.strip():
            return []
        
        candidates = []
        
        # Add custom FAQ titles
        for item in faq_items:
            candidates.append(("📝 " + item.question, item.question))
        
        # Add Helpshift titles
        for title in helpshift_titles:
            candidates.append(("🌐 " + title, title))
        
        # Use process.extract for efficient fuzzy matching
        results = process.extract(
            partial_query,
            [c[1] for c in candidates],
            scorer=fuzz.token_set_ratio,
            limit=max_results,
            score_cutoff=40  # Minimum score for autocomplete
        )
        
        # Map back to display titles
        matched = []
        for match_text, score, _ in results:
            for display, original in candidates:
                if original == match_text:
                    matched.append((display, score))
                    break
        
        return matched


# Test functions
def _test_fuzzy_search():
    """Test fuzzy search functionality."""
    from .synonyms import SynonymManager
    from .models import MOCK_FAQ_ITEMS, MOCK_HELPSHIFT_ARTICLES
    
    # Initialize
    synonym_mgr = SynonymManager()
    engine = FuzzySearchEngine(synonym_mgr, suggestion_threshold=75)
    
    print("=== Fuzzy Search Tests ===\n")
    
    # Test 1: Exact match
    print("Test 1: Exact match for 'ARR'")
    main, suggestions = engine.search_custom("ARR", MOCK_FAQ_ITEMS)
    assert main is not None
    assert "ARR" in main.title
    print(f"✓ Found: {main.title} (score: {main.score:.1f})")
    
    # Test 2: Synonym expansion
    print("\nTest 2: Synonym search 'money'")
    main, suggestions = engine.search_custom("money", MOCK_FAQ_ITEMS)
    assert main is not None or suggestions
    if main:
        print(f"✓ Found: {main.title} (score: {main.score:.1f})")
    else:
        print(f"✓ Suggestions: {len(suggestions)} items")
    
    # Test 3: Typo tolerance
    print("\nTest 3: Typo 'creditz' (should find 'credits')")
    main, suggestions = engine.search_custom("creditz", MOCK_FAQ_ITEMS)
    if main or suggestions:
        result = main or suggestions[0]
        print(f"✓ Found: {result.title} (score: {result.score:.1f})")
    
    # Test 4: Combined search
    print("\nTest 4: Combined search 'mission list'")
    main, suggestions = engine.search_combined(
        "mission list",
        MOCK_FAQ_ITEMS,
        MOCK_HELPSHIFT_ARTICLES
    )
    assert main is not None or suggestions
    if main:
        print(f"✓ Found: {main.title} from {main.source.value} (score: {main.score:.1f})")
    
    # Test 5: Autocomplete
    print("\nTest 5: Autocomplete for 'cre'")
    results = engine.autocomplete_search(
        "cre",
        MOCK_FAQ_ITEMS,
        [a.title for a in MOCK_HELPSHIFT_ARTICLES]
    )
    assert len(results) > 0
    print(f"✓ Found {len(results)} autocomplete suggestions:")
    for title, score in results[:3]:
        print(f"  - {title} ({score:.1f})")
    
    # Test 6: No results
    print("\nTest 6: No match query 'xyzabc123'")
    main, suggestions = engine.search_custom("xyzabc123", MOCK_FAQ_ITEMS)
    assert main is None
    assert len(suggestions) == 0
    print("✓ Correctly returned no results")
    
    # Test 7: Score threshold
    print("\nTest 7: Low score triggers suggestions")
    main, suggestions = engine.search_custom("something vague", MOCK_FAQ_ITEMS)
    if main is None and len(suggestions) > 0:
        print(f"✓ Correctly showed suggestions instead of weak main result")
    
    print("\n✅ All fuzzy search tests passed!")


if __name__ == "__main__":
    _test_fuzzy_search()
