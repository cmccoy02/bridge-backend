import requests
from typing import List, Dict, Optional, Any
import logging
from config import Config

class MetricsRepository:
    """
    Repository class for handling metrics data with Supabase REST API integration.
    Implements the repository pattern for database operations using direct REST calls.
    """
    
    def __init__(self):
        """Initialize Supabase REST API connection and set up logging."""
        try:
            self.base_url = f"{Config.SUPABASE_URL}/rest/v1"
            self.headers = {
                'apikey': Config.SUPABASE_KEY,
                'Authorization': f'Bearer {Config.SUPABASE_KEY}',
                'Content-Type': 'application/json',
                'Prefer': 'return=representation'
            }
            self.table_name = "metrics"
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)
        except Exception as e:
            self.logger.error(f"Failed to initialize Supabase REST API connection: {e}")
            raise
    
    def create_metric(self, metric_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a new metric record using Supabase REST API.
        
        Args:
            metric_data: Dictionary containing metric information with required fields:
                - repoID (uuid): Repository ID
                - importID (uuid): Import ID
                - commitHistScore (int): Commit history score (non-nullable)
                - complexityScore (int): Complexity score (non-nullable)
                - churnScore (int, optional): Churn score
                - totalScore (int, optional): Total score
                - packageVersionScore (int, optional): Package version score
            
        Returns:
            Created metric record or None if failed
        """
        try:
            # Validate required fields
            required_fields = ['repoID', 'importID', 'commitHistScore', 'complexityScore']
            for field in required_fields:
                if field not in metric_data:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate that required scores are integers
            if not isinstance(metric_data['commitHistScore'], int):
                raise ValueError("commitHistScore must be an integer")
            if not isinstance(metric_data['complexityScore'], int):
                raise ValueError("complexityScore must be an integer")
            
            # Make REST API call to insert the metric
            url = f"{self.base_url}/{self.table_name}"
            response = requests.post(url, headers=self.headers, json=metric_data)
            
            if response.status_code == 201:
                created_metric = response.json()
                self.logger.info(f"Created metric: {metric_data['repoID']}-{metric_data['importID']}")
                return created_metric[0] if isinstance(created_metric, list) else created_metric
            else:
                self.logger.error(f"Failed to create metric: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error creating metric: {e}")
            return None
    
    def get_metric_by_keys(self, repo_id: str, import_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a metric by its composite primary key using REST API.
        
        Args:
            repo_id: The repository ID (uuid)
            import_id: The import ID (uuid)
            
        Returns:
            Metric record or None if not found
        """
        try:
            url = f"{self.base_url}/{self.table_name}"
            params = {
                'repoID': f'eq.{repo_id}',
                'importID': f'eq.{import_id}',
                'select': '*'
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    return data[0]
                else:
                    self.logger.info(f"Metric with repoID {repo_id} and importID {import_id} not found")
                    return None
            else:
                self.logger.error(f"Failed to retrieve metric: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving metric {repo_id}-{import_id}: {e}")
            return None
    
    def get_metrics_by_repo(self, repo_id: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve all metrics for a specific repository using REST API.
        
        Args:
            repo_id: The repository ID (uuid)
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of metric records for the repository
        """
        try:
            url = f"{self.base_url}/{self.table_name}"
            params = {
                'repoID': f'eq.{repo_id}',
                'select': '*',
                'limit': limit,
                'offset': offset
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to retrieve metrics for repo {repo_id}: {response.status_code} - {response.text}")
                return []
            
        except Exception as e:
            self.logger.error(f"Error retrieving metrics for repo {repo_id}: {e}")
            return []
    
    def get_metrics_by_import(self, import_id: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve all metrics for a specific import using REST API.
        
        Args:
            import_id: The import ID (uuid)
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of metric records for the import
        """
        try:
            url = f"{self.base_url}/{self.table_name}"
            params = {
                'importID': f'eq.{import_id}',
                'select': '*',
                'limit': limit,
                'offset': offset
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to retrieve metrics for import {import_id}: {response.status_code} - {response.text}")
                return []
            
        except Exception as e:
            self.logger.error(f"Error retrieving metrics for import {import_id}: {e}")
            return []
    
    def get_all_metrics(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Retrieve all metrics with pagination using REST API.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of metric records
        """
        try:
            url = f"{self.base_url}/{self.table_name}"
            params = {
                'select': '*',
                'limit': limit,
                'offset': offset
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to retrieve metrics: {response.status_code} - {response.text}")
                return []
            
        except Exception as e:
            self.logger.error(f"Error retrieving metrics: {e}")
            return []
    
    def get_metrics_by_score_range(self, score_field: str, min_score: int, max_score: int) -> List[Dict[str, Any]]:
        """
        Retrieve metrics within a specific score range using REST API.
        
        Args:
            score_field: The score field to filter by (e.g., 'totalScore', 'complexityScore')
            min_score: Minimum score value
            max_score: Maximum score value
            
        Returns:
            List of metric records within the score range
        """
        try:
            valid_score_fields = ['commitHistScore', 'complexityScore', 'churnScore', 'totalScore', 'packageVersionScore']
            if score_field not in valid_score_fields:
                raise ValueError(f"Invalid score field. Must be one of: {valid_score_fields}")
            
            url = f"{self.base_url}/{self.table_name}"
            params = {
                'select': '*',
                score_field: f'gte.{min_score}',
                f'{score_field}': f'lte.{max_score}'
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to retrieve metrics by score range: {response.status_code} - {response.text}")
                return []
            
        except Exception as e:
            self.logger.error(f"Error retrieving metrics by score range: {e}")
            return []
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get a summary of metrics including count and basic statistics using REST API.
        
        Returns:
            Dictionary containing metrics summary
        """
        try:
            # Get total count
            url = f"{self.base_url}/{self.table_name}"
            count_params = {
                'select': 'count',
                'count': 'exact'
            }
            
            count_response = requests.get(url, headers=self.headers, params=count_params)
            
            total_count = 0
            if count_response.status_code == 200:
                # Extract count from response headers
                count_header = count_response.headers.get('content-range')
                if count_header:
                    total_count = int(count_header.split('/')[-1])
            
            # Get all metrics for calculating averages
            all_metrics = self.get_all_metrics(limit=1000)  # Adjust limit as needed
            
            if all_metrics:
                avg_scores = {}
                score_fields = ['commitHistScore', 'complexityScore', 'churnScore', 'totalScore', 'packageVersionScore']
                
                for field in score_fields:
                    valid_scores = [metric[field] for metric in all_metrics if metric.get(field) is not None]
                    if valid_scores:
                        avg_scores[f"avg_{field}"] = sum(valid_scores) / len(valid_scores)
                    else:
                        avg_scores[f"avg_{field}"] = None
            
            return {
                "total_count": total_count,
                "average_scores": avg_scores if 'avg_scores' in locals() else {}
            }
            
        except Exception as e:
            self.logger.error(f"Error getting metrics summary: {e}")
            return {"total_count": 0, "average_scores": {}}
    
    def get_repo_metrics_summary(self, repo_id: str) -> Dict[str, Any]:
        """
        Get a summary of metrics for a specific repository using REST API.
        
        Args:
            repo_id: The repository ID (uuid)
            
        Returns:
            Dictionary containing repository metrics summary
        """
        try:
            metrics = self.get_metrics_by_repo(repo_id)
            
            if not metrics:
                return {"repo_id": repo_id, "metric_count": 0, "average_scores": {}}
            
            avg_scores = {}
            score_fields = ['commitHistScore', 'complexityScore', 'churnScore', 'totalScore', 'packageVersionScore']
            
            for field in score_fields:
                valid_scores = [metric[field] for metric in metrics if metric.get(field) is not None]
                if valid_scores:
                    avg_scores[f"avg_{field}"] = sum(valid_scores) / len(valid_scores)
                else:
                    avg_scores[f"avg_{field}"] = None
            
            return {
                "repo_id": repo_id,
                "metric_count": len(metrics),
                "average_scores": avg_scores
            }
            
        except Exception as e:
            self.logger.error(f"Error getting repo metrics summary for {repo_id}: {e}")
            return {"repo_id": repo_id, "metric_count": 0, "average_scores": {}}
