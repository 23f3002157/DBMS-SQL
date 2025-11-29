from typing import List, Dict
import json
import pandas as pd

class Evaluator:
    def __init__(self):
        self.ground_truth = {
            "Who has the highest salary?": "Employee with highest salary is around $150k+",
            "Who manages the most employees?": "Managers with multiple direct reports",
            "Show engineering team salaries": "Engineering team salary range",
            "What is the average performance score?": "Average around 4.0-4.5",
            "Who was hired in 2023?": "Employees hired in 2023"
        }
    
    def run_evaluation(self, sql_results: List[Dict], kg_results: List[Dict]) -> Dict:
        """Run comprehensive evaluation"""
        all_results = sql_results + kg_results
        
        # Simple accuracy metrics
        scores = []
        for result in all_results:
            question = result['question']
            pred = result['final_answer'].lower()
            gold = self.ground_truth.get(question, "").lower()
            
            # Simple token overlap
            pred_tokens = set(pred.split())
            gold_tokens = set(gold.split())
            overlap = len(pred_tokens.intersection(gold_tokens))
            
            scores.append({
                'method': result['method'],
                'question': question,
                'overlap': overlap / max(len(pred_tokens), 1)
            })
        
        df_scores = pd.DataFrame(scores)
        metrics = {
            'sql_avg': df_scores[df_scores['method'] == 'SQL+LLM']['overlap'].mean(),
            'kg_avg': df_scores[df_scores['method'] == 'KG+LLM']['overlap'].mean(),
            'improvement': ((df_scores[df_scores['method'] == 'KG+LLM']['overlap'].mean() - 
                           df_scores[df_scores['method'] == 'SQL+LLM']['overlap'].mean()) * 100)
        }
        
        winner = 'KG+LLM' if metrics['kg_avg'] > metrics['sql_avg'] else 'SQL+LLM'
        
        return {
            'metrics': metrics,
            'sql_sample': sql_results[:2],
            'kg_sample': kg_results[:2],
            'winner': winner
        }