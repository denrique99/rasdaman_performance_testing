import requests
from typing import Optional, Dict, Any, Union
from datetime import datetime, timedelta

class OpenEOClient:
    """Base client for OpenEO API interactions"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        """
        Initialize OpenEO client
        
        Args:
            base_url (str): Base URL for the OpenEO API
        """
        self.base_url = base_url.rstrip('/')
        
    def make_request(
        self, 
        endpoint: str, 
        method: str = 'GET', 
        data: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request to OpenEO API
        
        Args:
            endpoint (str): API endpoint
            method (str): HTTP method (GET, POST, DELETE, PATCH)
            data (dict, optional): Data to send with request
            
        Returns:
            dict: Response data or None if request fails
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            response = requests.request(method=method, url=url, json=data)
            response.raise_for_status()
            return response.json() if response.content else None
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            raise OpenEOApiError(error_msg)

    def get_collections(self) -> Dict[str, Any]:
        """Get available collections"""
        return self.make_request('collections')

    def get_collection_details(self, collection_id: str) -> Dict[str, Any]:
        """Get details for specific collection"""
        return self.make_request(f'collections/{collection_id}')

    def get_processes(self) -> Dict[str, Any]:
        """Get available processes"""
        return self.make_request('processes')

    def get_jobs(self) -> Dict[str, Any]:
        """Get all jobs"""
        return self.make_request('jobs')

    def create_job(
        self,
        title: str,
        collection_id: str,
        spatial_extent: Dict[str, float],
        temporal_extent: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Create new processing job
        
        Args:
            title (str): Job title
            collection_id (str): Collection ID to process
            spatial_extent (dict): Spatial boundaries (west, east, north, south)
            temporal_extent (list, optional): Start and end dates
        """
        if temporal_extent is None:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            temporal_extent = [
                start_date.isoformat() + "Z",
                end_date.isoformat() + "Z"
            ]

        job_data = {
            "title": title,
            "process": {
                "process_graph": {
                    "load_data": {
                        "process_id": "load_collection",
                        "arguments": {
                            "id": collection_id,
                            "spatial_extent": spatial_extent,
                            "temporal_extent": temporal_extent
                        }
                    }
                }
            }
        }
        
        return self.make_request('jobs', method='POST', data=job_data)

    def start_job(self, job_id: str) -> Dict[str, Any]:
        """Start specific job"""
        return self.make_request(f'jobs/{job_id}/results', method='POST')

    def get_job_results(self, job_id: str) -> Dict[str, Any]:
        """Get results of finished job"""
        return self.make_request(f'jobs/{job_id}/results')

    def delete_job(self, job_id: str) -> bool:
        """Delete specific job"""
        return self.make_request(f'jobs/{job_id}', method='DELETE') is None


class OpenEOApiError(Exception):
    """Custom exception for OpenEO API errors"""
    pass


# Utility functions that can be used by both CLI and GUI
def format_timestamp(timestamp: str) -> str:
    """Format ISO timestamp for display"""
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return timestamp

def validate_spatial_extent(
    west: float, 
    east: float, 
    north: float, 
    south: float
) -> Dict[str, float]:
    """
    Validate and return spatial extent
    
    Args:
        west (float): Western boundary
        east (float): Eastern boundary
        north (float): Northern boundary
        south (float): Southern boundary
        
    Returns:
        dict: Validated spatial extent
        
    Raises:
        ValueError: If spatial extent is invalid
    """
    if not (-180 <= west <= 180 and -180 <= east <= 180):
        raise ValueError("Longitude must be between -180 and 180 degrees")
    
    if not (-90 <= south <= 90 and -90 <= north <= 90):
        raise ValueError("Latitude must be between -90 and 90 degrees")
        
    if west >= east:
        raise ValueError("Western boundary must be less than eastern boundary")
        
    if south >= north:
        raise ValueError("Southern boundary must be less than northern boundary")
        
    return {
        "west": west,
        "east": east,
        "north": north,
        "south": south
    }

def validate_temporal_extent(
    start_date: Union[str, datetime], 
    end_date: Union[str, datetime]
) -> list:
    """
    Validate and return temporal extent
    
    Args:
        start_date: Start date (ISO string or datetime)
        end_date: End date (ISO string or datetime)
        
    Returns:
        list: Validated temporal extent
        
    Raises:
        ValueError: If temporal extent is invalid
    """
    # Convert strings to datetime if necessary
    if isinstance(start_date, str):
        start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    if isinstance(end_date, str):
        end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
    if start_date >= end_date:
        raise ValueError("Start date must be before end date")
        
    return [
        start_date.isoformat() + "Z",
        end_date.isoformat() + "Z"
    ]