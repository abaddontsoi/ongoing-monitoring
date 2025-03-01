from rapidfuzz import fuzz
from typing import List, Optional

def get_similarities(
    query: str,
    choices: List[str],
    case_sensitive: bool = True,
    order_sensitive: bool = True,
    threshold: float = 0,
    threshold_type: str = '>='
) -> List[float]:
    """
    Calculate similarity scores between a query string and a list of choices.
    
    Args:
        query: The string to compare against
        choices: List of strings to compare with
        case_sensitive: Whether comparison should be case-sensitive
        order_sensitive: Whether word order matters
        threshold: Similarity threshold value
        threshold_type: Threshold comparison type ('>=' or '>')
    
    Returns:
        List of similarity scores (0-100) for matches that meet the threshold
    """
    results = []
    
    for choice in choices:
        if order_sensitive:
            score = fuzz.ratio(query, choice)
        else:
            score = fuzz.token_sort_ratio(query, choice)
            
        if not case_sensitive:
            score = fuzz.ratio(query.lower(), choice.lower())
            
        if threshold_type == '>=' and score >= threshold:
            results.append(score)
        elif threshold_type == '>' and score > threshold:
            results.append(score)
            
    return results 