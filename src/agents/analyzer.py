import asyncio
from typing import Dict, Any
from src.agents.base import BaseAgent
from src.models import AnalysisReport, OutputChunk
from src.utils.logger import get_logger

logger = get_logger("analyzer_agent")

class AnalyzerAgent(BaseAgent):
    """
    Analyses retrieved data for contradictions and computes confidence scores. We wrote a basic
    substring contradiction checker here because building a full LLM fact-checking pipeline
    locally would take way too long.
    """

    def __init__(self, confidence_threshold: float = 0.90) -> None:
        self.confidence_threshold = confidence_threshold

    async def run(self, inputs: Dict[str, Any], queue: asyncio.Queue[Any]) -> Dict[str, Any]:
        """
        Executes Analyzer Agent.
        
        Inputs support:
          - "data": Output dict from RetrieverAgent.
        """
        step_id = inputs.get("step_id", "analyzer")
        
        await queue.put(OutputChunk(
            step_id=step_id,
            content="Starting factual analysis, sentiment modeling, and contradiction checks...",
            status="running"
        ))
        
        retriever_output = inputs.get("data", {})
        if not retriever_output:
            logger.error("No input data provided to analyzer", step_id=step_id)
            raise ValueError("Analyzer requires retrieval data inside the 'data' field.")
            
        results = retriever_output.get("results", [])
        retrieval_status = retriever_output.get("status", "success")
        
        if not results:
            logger.warn("Empty results in analyzer input", step_id=step_id)
            report = AnalysisReport(
                sentiment="neutral",
                contradictions=["No data retrieved to analyze"],
                summary="Analysis could not be performed due to lack of retrieved source data.",
                confidence=0.0,
                warning="Degraded: empty retrieval dataset"
            )
            return report.model_dump()
            
        # 1. Confidence Thresholding
        low_confidence_flag = False
        lowest_conf = 1.0
        total_conf = 0.0
        
        for r in results:
            conf = r.get("confidence", 1.0)
            total_conf += conf
            if conf < lowest_conf:
                lowest_conf = conf
            if conf < self.confidence_threshold:
                low_confidence_flag = True
                
        avg_confidence = total_conf / len(results)
        
        # 2. Contradiction Detection
        contradictions = []
        for r in results:
            content = r.get("content", "").lower()
            
            # Look for keyword indicators of policy or data tension
            if "but" in content or "however" in content or "contradict" in content:
                # Extract sentence containing contradiction
                sentences = r.get("content", "").split(".")
                for s in sentences:
                    s_lower = s.lower()
                    if any(kw in s_lower for kw in ["but", "however", "contradict", "offset", "record"]):
                        contradictions.append(f"Contradiction flagged in '{r.get('source')}': {s.strip()}")
                        
        # 3. Sentiment Analysis (Keyword scoring)
        positives = ["growth", "upward", "accelerating", "accuracy", "advancement", "breakthrough", "benefit", "green"]
        negatives = ["contradiction", "fossil", "coal", "fail", "invalid", "degraded", "conflict", "warn"]
        
        pos_count = 0
        neg_count = 0
        for r in results:
            content = r.get("content", "").lower()
            pos_count += sum(1 for w in positives if w in content)
            neg_count += sum(1 for w in negatives if w in content)
            
        if pos_count > neg_count + 1:
            sentiment = "positive"
        elif neg_count > pos_count + 1:
            sentiment = "negative"
        else:
            sentiment = "mixed"

        # 4. Statistical/Factual Summary
        summaries = []
        for r in results:
            summaries.append(f"- From {r.get('source')}: {r.get('content')}")
        summary_str = "Factual summary of retrieved topics:\n" + "\n".join(summaries)
        
        # Determine warning messages
        warning_msg = None
        if low_confidence_flag:
            warning_msg = f"Data contains sources below confidence threshold ({lowest_conf:.2f} < {self.confidence_threshold:.2f})."
            await queue.put(OutputChunk(
                step_id=step_id,
                content=f"⚠️ Warning: {warning_msg}",
                status="warning"
            ))
            
        if retrieval_status == "degraded":
            degrade_warning = "Retrieved dataset is in a degraded state due to partial query failures."
            warning_msg = f"{warning_msg} | {degrade_warning}" if warning_msg else degrade_warning
            
        # Simulate processing delay
        await asyncio.sleep(0.1)
        
        report = AnalysisReport(
            sentiment=sentiment,
            contradictions=contradictions,
            summary=summary_str,
            confidence=round(avg_confidence, 2),
            warning=warning_msg
        )
        
        await queue.put(OutputChunk(
            step_id=step_id,
            content=f"Analysis complete. Sentiment: '{sentiment}'. Identified {len(contradictions)} contradiction(s).",
            status="completed"
        ))
        
        return report.model_dump()
