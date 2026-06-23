from src.parser import TaskParserAgent

def test_parser_happy_path_protein_folding() -> None:
    parser = TaskParserAgent()
    query = "Analyze recent AI breakthroughs in protein folding, summarize sentiment, and write a blog post outline"
    result = parser.parse(query)
    
    assert result.is_valid is True
    assert result.intent == "orchestration"
    assert "protein folding" in result.entities

def test_parser_happy_path_stocks() -> None:
    parser = TaskParserAgent()
    query = "Retrieve stock prices for AAPL, GOOGL, INVALID_TICKET, MSFT and analyze trends"
    result = parser.parse(query)
    
    assert result.is_valid is True
    assert result.intent == "retrieval_analysis"
    assert "AAPL" in result.entities
    assert "GOOGL" in result.entities
    assert "MSFT" in result.entities
    assert "INVALID_TICKET" in result.entities

def test_parser_happy_path_climate() -> None:
    parser = TaskParserAgent()
    query = "Compare climate policies of EU, USA, and China from 2023-2024. Identify contradictions. Write a balanced report."
    result = parser.parse(query)
    
    assert result.is_valid is True
    assert result.intent == "orchestration"
    assert "EU" in result.entities
    assert "USA" in result.entities
    assert "China" in result.entities

def test_parser_empty_query() -> None:
    parser = TaskParserAgent()
    result = parser.parse("   ")
    
    assert result.is_valid is False
    assert result.intent == "invalid"
    assert result.clarifying_question is not None
    assert "empty" in result.ambiguity_explanation

def test_parser_short_query() -> None:
    parser = TaskParserAgent()
    result = parser.parse("hello")
    
    assert result.is_valid is False
    assert result.intent == "invalid"
    assert "too short" in result.ambiguity_explanation
