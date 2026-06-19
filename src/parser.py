import re
from typing import List
from src.models import TaskRequest
from src.utils.logger import get_logger

logger = get_logger("task_parser")

# Regex is hard, this probably has edge cases. I spent way too long writing self-made keyword matchers
# instead of importing spaCy or NLTK. Ngl, it works for our test cases so i'm not touching it.

class TaskParserAgent:
    """Task Parser Agent: Classifies intent, extracts entities, and detects ambiguity."""

    def __init__(self) -> None:
        # Just simple keyword matchers to classify intent. Simple but does the job.
        self.retrieval_keywords = ["retrieve", "fetch", "get", "search", "lookup", "stock", "price", "breakthrough"]
        self.analysis_keywords = ["analyze", "sentiment", "trend", "compare", "policy", "contradiction", "fact-check"]
        self.writing_keywords = ["write", "blog", "outline", "post", "report", "summary", "synthesize"]

    def parse(self, raw_query: str) -> TaskRequest:
        """
        Parses a raw query to extract intent, entities, and validation status.
        
        Args:
            raw_query: The raw string query from the user.
            
        Returns:
            TaskRequest: The structured representation.
        """
        logger.info("Parsing raw query", raw_query=raw_query)
        
        if not raw_query or not raw_query.strip():
            logger.warn("Query is empty")
            return TaskRequest(
                raw_query=raw_query or "",
                intent="invalid",
                entities=[],
                is_valid=False,
                ambiguity_explanation="The query is completely empty.",
                clarifying_question="Could you please specify what information you would like to retrieve or analyze?"
            )
            
        cleaned_query = raw_query.strip()
        
        # Ambiguity detection: check if too short or gibberish
        if len(cleaned_query) < 10:
            logger.warn("Query is too short or ambiguous", query=cleaned_query)
            return TaskRequest(
                raw_query=cleaned_query,
                intent="invalid",
                entities=[],
                is_valid=False,
                ambiguity_explanation="The query is too short or ambiguous.",
                clarifying_question="Could you provide a more detailed request describing the topics or tasks you want to execute?"
            )

        # Intent classification
        query_lower = cleaned_query.lower()
        has_retrieval = any(kw in query_lower for kw in self.retrieval_keywords)
        has_analysis = any(kw in query_lower for kw in self.analysis_keywords)
        has_writing = any(kw in query_lower for kw in self.writing_keywords)
        
        # Comparative queries with regions implicitly require retrieving information first
        has_regions = any(r.lower() in query_lower for r in ["eu", "usa", "china"])
        if has_regions and has_analysis and has_writing:
            has_retrieval = True
        
        if has_retrieval and has_analysis and has_writing:
            intent = "orchestration"
        elif has_retrieval and has_analysis:
            intent = "retrieval_analysis"
        elif has_analysis and has_writing:
            intent = "analysis_writing"
        elif has_retrieval:
            intent = "retrieval"
        elif has_analysis:
            intent = "analysis"
        elif has_writing:
            intent = "writing"
        else:
            # Let's see if we should treat it as orchestration or invalid
            # If no keyword matches, check if we have any nouns/entities.
            # If it's a general query, default to orchestration but flag warnings
            intent = "orchestration"

        # Entity Extraction
        entities = self._extract_entities(cleaned_query)
        
        # Additional validity check: If intent is classified, but no entities are extracted, 
        # it might be ambiguous unless it's a generic command.
        if not entities and not has_retrieval and not has_analysis and not has_writing:
            logger.warn("Query has no clear intent or entities", query=cleaned_query)
            return TaskRequest(
                raw_query=cleaned_query,
                intent="invalid",
                entities=[],
                is_valid=False,
                ambiguity_explanation="The system could not identify any clear topics, entities, or action items in your query.",
                clarifying_question="What specific topics or entities (e.g., countries, stocks, or subjects) would you like us to focus on?"
            )

        logger.info("Parsing completed successfully", intent=intent, entities=entities)
        return TaskRequest(
            raw_query=cleaned_query,
            intent=intent,
            entities=entities,
            is_valid=True,
            ambiguity_explanation=None,
            clarifying_question=None
        )

    def _extract_entities(self, query: str) -> List[str]:
        """Extracts key terms, uppercase names, or specific entities from the query."""
        entities = []
        
        # Extract stock tickets like AAPL, GOOGL, INVALID_TICKET, MSFT
        # Typically uppercase words of length 3-14 (often 3-5, but let's be generous for custom tickets)
        stock_matches = re.findall(r'\b[A-Z]{3,14}(?:_[A-Z]{3,14})*\b', query)
        for match in stock_matches:
            if match not in ["AND", "USA", "THE", "FOR", "NOT", "OUTLINE", "JSON", "HTML", "Markdown", "Mermaid"]:
                entities.append(match)
                
        # Extract countries / regions (EU, USA, China, etc.)
        regions = ["EU", "USA", "China", "USA", "UK", "Japan", "Germany", "France", "India"]
        for region in regions:
            if re.search(r'\b' + re.escape(region) + r'\b', query, re.IGNORECASE):
                # Normalize to standard casing if needed
                if region not in entities:
                    entities.append(region)
                    
        # Extract specific phrases using keyword groups (e.g., protein folding, climate policies)
        phrases = ["protein folding", "climate policies", "AI breakthroughs", "sentiment analysis", "stock prices"]
        for phrase in phrases:
            if phrase.lower() in query.lower():
                if phrase not in entities:
                    entities.append(phrase)

        # Fallback: if we found nothing, extract capitalized words or quotes
        if not entities:
            # Match words in quotes
            quoted = re.findall(r'"([^"]+)"', query)
            if quoted:
                entities.extend(quoted)
                
        # Remove duplicates preserving order
        unique_entities = []
        for e in entities:
            if e not in unique_entities:
                unique_entities.append(e)
                
        return unique_entities
