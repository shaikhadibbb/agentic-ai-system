import asyncio
from typing import Dict, Any, List
from src.agents.base import BaseAgent
from src.models import WriterResponse, OutputChunk
from src.utils.logger import get_logger

logger = get_logger("writer_agent")

class WriterAgent(BaseAgent):
    """
    Takes all the structured analysis reports and formats them into clean markdown. 
    Simulates streaming tokens using asyncio.sleep to look nice in the frontend.
    """

    def __init__(self) -> None:
        pass

    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue[Any]) -> Dict[str, Any]:
        """
        Executes Writer Agent.
        
        Inputs support:
          - "report": Dict (Single AnalysisReport output)
          - "reports": List[Dict] (Multiple AnalysisReport outputs from parallel steps)
          - "query": The original raw query string.
        """
        step_id = inputs.get("step_id", "writer")
        query = inputs.get("query", "General Query")
        
        await queue.put(OutputChunk(
            step_id=step_id,
            content="Synthesizing final document and generating streaming response...",
            status="running"
        ))
        
        # 1. Gather all reports and extract details
        reports: List[Dict[str, Any]] = []
        if "report" in inputs and inputs["report"]:
            reports = [inputs["report"]]
        elif "reports" in inputs and inputs["reports"]:
            reports = inputs["reports"]
            
        if not reports:
            logger.error("No reports provided to writer", step_id=step_id)
            raise ValueError("Writer Agent requires at least one 'report' or 'reports' input.")

        # 2. Extract facts, contradictions, and build citations
        citations = []
        citations_text = []
        contradictions = []
        summaries_block = []
        
        citation_counter = 1
        
        for rep in reports:
            # Gather contradictions
            if rep.get("contradictions"):
                contradictions.extend(rep["contradictions"])
                
            # Parse sources from summary text to build citation objects
            summary_lines = rep.get("summary", "").split("\n")
            for line in summary_lines:
                if line.startswith("- From "):
                    # Extract Source: Content
                    # Example: "- From Bloomberg Finance: AAPL stock price is..."
                    parts = line[7:].split(": ", 1)
                    if len(parts) == 2:
                        source_name = parts[0].strip()
                        claim = parts[1].strip()
                        
                        citation_key = f"[{citation_counter}]"
                        citations.append({
                            "key": citation_key,
                            "source": source_name,
                            "claim": claim
                        })
                        
                        citations_text.append(f"{citation_key} **{source_name}**: *\"{claim}\"*")
                        summaries_block.append(f"- {claim} {citation_key}")
                        citation_counter += 1
                        
        # 3. Formulate the Markdown text
        markdown_lines = []
        markdown_lines.append("# Executive Report: Multi-Agent Analysis")
        markdown_lines.append(f"**Query**: *\"{query}\"*")
        markdown_lines.append("")
        
        markdown_lines.append("## 1. Synthesis of Key Findings")
        if summaries_block:
            markdown_lines.extend(summaries_block)
        else:
            markdown_lines.append("No specific factual findings were synthesized from the source reports.")
        markdown_lines.append("")
        
        # Add sentiment synthesis
        sentiments = [rep.get("sentiment", "mixed") for rep in reports]
        overall_sentiment = max(set(sentiments), key=sentiments.count) if sentiments else "neutral"
        markdown_lines.append(f"**Overall Synthesized Sentiment**: `{overall_sentiment.upper()}`")
        markdown_lines.append("")
        
        markdown_lines.append("## 2. Contradiction and Policy Friction Analysis")
        if contradictions:
            for contra in contradictions:
                markdown_lines.append(f"- ⚠️ {contra}")
        else:
            markdown_lines.append("No policy frictions or data contradictions were identified across sources.")
        markdown_lines.append("")
        
        markdown_lines.append("## 3. Sources and Citations")
        if citations_text:
            for cite in citations_text:
                markdown_lines.append(f"- {cite}")
        else:
            markdown_lines.append("No external citations were linked.")
        markdown_lines.append("")
        
        final_markdown = "\n".join(markdown_lines)
        
        # 4. Stream response chunk-by-chunk to the queue
        # Simulate token-by-token or word-by-word streaming
        words = final_markdown.split(" ")
        chunk_size = 5 # yield 5 words at a time
        
        for i in range(0, len(words), chunk_size):
            chunk = " ".join(words[i:i+chunk_size]) + " "
            # Emit token stream
            await queue.put(OutputChunk(
                step_id=step_id,
                content=chunk,
                status="running"
            ))
            # Sleep to simulate network latency / streaming generation
            await asyncio.sleep(0.02)
            
        await queue.put(OutputChunk(
            step_id=step_id,
            content="Final document synthesized successfully.",
            status="completed"
        ))
        
        response = WriterResponse(
            markdown_content=final_markdown,
            json_content={
                "overall_sentiment": overall_sentiment,
                "contradictions_count": len(contradictions),
                "citations_count": len(citations)
            },
            citations=citations
        )
        
        return response.model_dump()
