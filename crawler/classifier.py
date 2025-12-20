"""
AI Content Classification - Automatic page categorization using NLP.

Features:
- Zero-shot text classification (no training required)
- Keyword extraction
- Sentiment analysis
- Named entity recognition
- Content summarization

Uses lightweight models that work on CPU without GPU requirements.
"""

import re
from collections import Counter
from typing import Optional
from dataclasses import dataclass, field

# Try to import ML libraries, fall back to rule-based if not available
try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False

try:
    import nltk
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.corpus import stopwords
    from nltk.tag import pos_tag
    HAS_NLTK = True
except ImportError:
    HAS_NLTK = False


# Default categories for classification
DEFAULT_CATEGORIES = [
    "Technology",
    "Business",
    "News",
    "Blog",
    "E-commerce",
    "Entertainment",
    "Education",
    "Health",
    "Sports",
    "Travel",
    "Food",
    "Finance",
    "Government",
    "Science",
    "Art & Design"
]


@dataclass
class ClassificationResult:
    """Result of content classification."""
    url: str
    title: Optional[str] = None
    
    # Classification
    category: str = "Unknown"
    category_confidence: float = 0.0
    secondary_categories: list = field(default_factory=list)
    
    # Keywords
    keywords: list = field(default_factory=list)
    
    # Sentiment
    sentiment: str = "neutral"  # positive, negative, neutral
    sentiment_score: float = 0.0
    
    # Entities
    entities: list = field(default_factory=list)
    
    # Summary
    summary: str = ""
    
    # Metadata
    word_count: int = 0
    reading_time_minutes: float = 0.0


class RuleBasedClassifier:
    """
    Fallback rule-based classifier when ML libraries aren't available.
    Uses keyword matching for classification.
    """
    
    CATEGORY_KEYWORDS = {
        "Technology": [
            "software", "hardware", "programming", "code", "developer", "api",
            "cloud", "data", "ai", "machine learning", "app", "tech", "computer",
            "digital", "algorithm", "startup", "silicon valley", "innovation"
        ],
        "Business": [
            "company", "market", "revenue", "profit", "ceo", "startup", "investment",
            "stock", "enterprise", "corporate", "management", "strategy", "growth"
        ],
        "News": [
            "breaking", "latest", "update", "report", "announced", "today",
            "yesterday", "official", "statement", "press", "media"
        ],
        "Blog": [
            "personal", "opinion", "thoughts", "my experience", "i think",
            "review", "journey", "story", "lessons learned"
        ],
        "E-commerce": [
            "buy", "shop", "cart", "price", "discount", "sale", "order",
            "shipping", "product", "store", "checkout", "add to cart"
        ],
        "Entertainment": [
            "movie", "music", "game", "celebrity", "streaming", "show",
            "series", "album", "concert", "entertainment", "fun"
        ],
        "Education": [
            "learn", "course", "tutorial", "study", "university", "school",
            "student", "teacher", "education", "training", "lesson", "class"
        ],
        "Health": [
            "health", "medical", "doctor", "hospital", "treatment", "symptom",
            "disease", "wellness", "fitness", "diet", "nutrition", "exercise"
        ],
        "Sports": [
            "game", "team", "player", "score", "championship", "league",
            "football", "basketball", "soccer", "match", "tournament"
        ],
        "Travel": [
            "travel", "vacation", "hotel", "flight", "destination", "trip",
            "tourism", "adventure", "explore", "booking", "resort"
        ],
        "Food": [
            "recipe", "restaurant", "cooking", "food", "meal", "ingredient",
            "chef", "cuisine", "dining", "taste", "delicious"
        ],
        "Finance": [
            "money", "bank", "loan", "credit", "investment", "financial",
            "budget", "savings", "insurance", "mortgage", "crypto", "bitcoin"
        ],
        "Science": [
            "research", "study", "scientist", "experiment", "discovery",
            "physics", "chemistry", "biology", "space", "nature"
        ]
    }
    
    SENTIMENT_WORDS = {
        "positive": [
            "good", "great", "excellent", "amazing", "wonderful", "best",
            "love", "awesome", "fantastic", "perfect", "happy", "success",
            "beautiful", "brilliant", "outstanding", "incredible"
        ],
        "negative": [
            "bad", "terrible", "awful", "worst", "hate", "poor", "fail",
            "wrong", "horrible", "disappointing", "ugly", "broken", "sad",
            "angry", "frustrated", "problem", "issue", "error"
        ]
    }
    
    def classify(self, text: str, title: str = "") -> ClassificationResult:
        """Classify text using keyword matching."""
        text_lower = (title + " " + text).lower()
        words = re.findall(r'\b\w+\b', text_lower)
        word_count = len(words)
        
        # Category classification
        category_scores = {}
        for category, keywords in self.CATEGORY_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                category_scores[category] = score
        
        if category_scores:
            sorted_categories = sorted(
                category_scores.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            category = sorted_categories[0][0]
            max_score = sorted_categories[0][1]
            total_score = sum(category_scores.values())
            confidence = max_score / total_score if total_score > 0 else 0
            secondary = [c for c, _ in sorted_categories[1:3]]
        else:
            category = "Unknown"
            confidence = 0.0
            secondary = []
        
        # Sentiment analysis
        pos_count = sum(1 for w in self.SENTIMENT_WORDS["positive"] if w in text_lower)
        neg_count = sum(1 for w in self.SENTIMENT_WORDS["negative"] if w in text_lower)
        
        if pos_count > neg_count * 1.5:
            sentiment = "positive"
            sentiment_score = min(1.0, pos_count / (pos_count + neg_count + 1))
        elif neg_count > pos_count * 1.5:
            sentiment = "negative"
            sentiment_score = -min(1.0, neg_count / (pos_count + neg_count + 1))
        else:
            sentiment = "neutral"
            sentiment_score = 0.0
        
        # Keyword extraction (most frequent non-stopwords)
        stopword_set = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "do", "does", "did", "will",
            "would", "could", "should", "may", "might", "can", "this",
            "that", "these", "those", "i", "you", "he", "she", "it",
            "we", "they", "what", "which", "who", "when", "where", "why",
            "how", "all", "each", "every", "both", "few", "more", "most",
            "other", "some", "such", "no", "not", "only", "same", "so",
            "than", "too", "very", "just", "but", "and", "or", "if",
            "because", "as", "until", "while", "of", "at", "by", "for",
            "with", "about", "against", "between", "into", "through",
            "during", "before", "after", "above", "below", "to", "from",
            "up", "down", "in", "out", "on", "off", "over", "under"
        }
        
        filtered_words = [w for w in words if len(w) > 3 and w not in stopword_set]
        word_freq = Counter(filtered_words)
        keywords = [word for word, _ in word_freq.most_common(10)]
        
        # Reading time (average 200 words per minute)
        reading_time = word_count / 200.0
        
        # Summary (first few sentences)
        sentences = re.split(r'[.!?]+', text)
        summary_sentences = [s.strip() for s in sentences[:3] if len(s.strip()) > 20]
        summary = ". ".join(summary_sentences)[:300]
        
        return ClassificationResult(
            url="",
            title=title,
            category=category,
            category_confidence=round(confidence, 2),
            secondary_categories=secondary,
            keywords=keywords,
            sentiment=sentiment,
            sentiment_score=round(sentiment_score, 2),
            entities=[],
            summary=summary,
            word_count=word_count,
            reading_time_minutes=round(reading_time, 1)
        )


class MLClassifier:
    """
    ML-based classifier using Hugging Face transformers.
    Uses zero-shot classification for flexibility.
    """
    
    def __init__(self):
        self._classifier = None
        self._sentiment = None
        self._summarizer = None
        self._ner = None
    
    def _load_classifier(self):
        if self._classifier is None and HAS_TRANSFORMERS:
            try:
                self._classifier = pipeline(
                    "zero-shot-classification",
                    model="facebook/bart-large-mnli",
                    device=-1  # CPU
                )
            except Exception:
                self._classifier = False
        return self._classifier
    
    def _load_sentiment(self):
        if self._sentiment is None and HAS_TRANSFORMERS:
            try:
                self._sentiment = pipeline(
                    "sentiment-analysis",
                    model="distilbert-base-uncased-finetuned-sst-2-english",
                    device=-1
                )
            except Exception:
                self._sentiment = False
        return self._sentiment
    
    def classify(self, text: str, title: str = "", 
                 categories: list = None) -> ClassificationResult:
        """Classify text using ML models."""
        if categories is None:
            categories = DEFAULT_CATEGORIES
        
        # Prepare text (limit length for performance)
        full_text = f"{title}. {text}"[:2000]
        
        result = ClassificationResult(url="", title=title)
        
        # Zero-shot classification
        classifier = self._load_classifier()
        if classifier and classifier is not False:
            try:
                output = classifier(full_text, categories, multi_label=True)
                result.category = output['labels'][0]
                result.category_confidence = round(output['scores'][0], 2)
                result.secondary_categories = output['labels'][1:3]
            except Exception:
                pass
        
        # Sentiment
        sentiment_analyzer = self._load_sentiment()
        if sentiment_analyzer and sentiment_analyzer is not False:
            try:
                sent_result = sentiment_analyzer(full_text[:500])[0]
                result.sentiment = sent_result['label'].lower()
                score = sent_result['score']
                result.sentiment_score = score if result.sentiment == 'positive' else -score
            except Exception:
                pass
        
        # Word count and reading time
        words = full_text.split()
        result.word_count = len(words)
        result.reading_time_minutes = round(len(words) / 200.0, 1)
        
        return result


class ContentClassifier:
    """
    Main classifier that uses ML if available, falls back to rules.
    """
    
    def __init__(self, use_ml: bool = True):
        self.use_ml = use_ml and HAS_TRANSFORMERS
        self._ml_classifier = None
        self._rule_classifier = RuleBasedClassifier()
    
    def classify(self, text: str, url: str = "", title: str = "",
                 categories: list = None) -> ClassificationResult:
        """
        Classify content and extract insights.
        
        Args:
            text: The main text content
            url: The source URL
            title: The page title
            categories: Optional custom categories
        
        Returns:
            ClassificationResult with category, sentiment, keywords, etc.
        """
        if self.use_ml and self._ml_classifier is None:
            self._ml_classifier = MLClassifier()
        
        if self.use_ml and self._ml_classifier:
            result = self._ml_classifier.classify(text, title, categories)
        else:
            result = self._rule_classifier.classify(text, title)
        
        result.url = url
        return result
    
    def classify_batch(self, items: list) -> list[ClassificationResult]:
        """
        Classify multiple items.
        
        Args:
            items: List of dicts with 'text', 'url', 'title' keys
        
        Returns:
            List of ClassificationResult objects
        """
        return [
            self.classify(
                text=item.get('text', ''),
                url=item.get('url', ''),
                title=item.get('title', '')
            )
            for item in items
        ]


# Global classifier instance
_classifier: Optional[ContentClassifier] = None


def get_classifier(use_ml: bool = True) -> ContentClassifier:
    """Get or create the global classifier instance."""
    global _classifier
    if _classifier is None:
        _classifier = ContentClassifier(use_ml=use_ml)
    return _classifier


def classify_content(text: str, url: str = "", title: str = "") -> dict:
    """
    Convenience function for quick classification.
    Returns dict suitable for API response.
    """
    classifier = get_classifier(use_ml=False)  # Use rules for speed
    result = classifier.classify(text, url, title)
    
    return {
        "url": result.url,
        "title": result.title,
        "category": result.category,
        "category_confidence": result.category_confidence,
        "secondary_categories": result.secondary_categories,
        "keywords": result.keywords,
        "sentiment": result.sentiment,
        "sentiment_score": result.sentiment_score,
        "summary": result.summary,
        "word_count": result.word_count,
        "reading_time_minutes": result.reading_time_minutes
    }
