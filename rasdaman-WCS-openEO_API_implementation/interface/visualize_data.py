import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import requests
import click
from rich.console import Console
from rich.panel import Panel
from interface import OpenEOClient
from rasterio.io import MemoryFile
import io

console = Console()

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import io
from rasterio.io import MemoryFile
from datetime import datetime
import traceback

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import requests
import io
from rasterio.io import MemoryFile
from datetime import datetime
import traceback

class DataVisualizer:
    def __init__(self, client=None):
        self.client = client or OpenEOClient()
        self.auth = ("rasadmin", "rasadmin")

    def get_job_data(self, job_id):
        """Fetch job results and metadata"""
        try:
            job_info = self.client.make_request(f'jobs/{job_id}')
            if not job_info:
                raise Exception("Could not fetch job information")

            results = self.client.make_request(f'jobs/{job_id}/results')
            if not results:
                raise Exception("Could not fetch job results")

            data_url = results.get('assets', {}).get('data', {}).get('href')
            if not data_url:
                raise Exception("No data URL in results")

            return data_url, job_info

        except Exception as e:
            raise Exception(f"Error fetching job data: {str(e)}")

    def format_timestamp(self, timestamp):
        """Format timestamp to be compatible with Rasdaman"""
        if isinstance(timestamp, str):
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            dt = timestamp
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def load_time_series_data(self, data_url, temporal_extent, spatial_extent):
        """Load time series data for the given spatial region"""
        try:
            values = []
            start_time = datetime.fromisoformat(temporal_extent[0].replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(temporal_extent[1].replace('Z', '+00:00'))
            
            timestamps = pd.date_range(start=start_time, end=end_time, periods=10)
            
            for timestamp in timestamps:
                timestamp_str = self.format_timestamp(timestamp)
                
                if '?' in data_url:
                    base_url = data_url + '&'
                else:
                    base_url = data_url + '?'
                
                modified_url = (
                    f"{base_url}FORMAT=image/tiff&"
                    f"SUBSET=ansi(\"{timestamp_str}\")&"
                    f"SUBSET=Lat({spatial_extent['south']},{spatial_extent['north']})&"
                    f"SUBSET=Long({spatial_extent['west']},{spatial_extent['east']})"
                )
                
                response = requests.get(modified_url, auth=self.auth)
                
                if response.status_code != 200:
                    raise Exception(f"HTTP Error {response.status_code}: {response.text[:500]}")

                with MemoryFile(io.BytesIO(response.content)) as memfile:
                    with memfile.open() as dataset:
                        data = dataset.read(1)
                        mean_value = np.nanmean(data)
                        values.append(mean_value)

            return timestamps, values

        except Exception as e:
            raise Exception(f"Error loading time series data: {str(e)}")

    def load_geotiff_data(self, data_url, temporal_extent):
        """Load GeoTIFF data for spatial visualization"""
        try:
            if '?' in data_url:
                base_url = data_url + '&'
            else:
                base_url = data_url + '?'
                
            timestamp = self.format_timestamp(temporal_extent[0])
            
            modified_url = (
                f"{base_url}FORMAT=image/tiff&"
                f"SUBSET=ansi(\"{timestamp}\")"
            )
            
            response = requests.get(modified_url, auth=self.auth)
            if response.status_code != 200:
                raise Exception(f"HTTP Error {response.status_code}: {response.text[:500]}")

            with MemoryFile(io.BytesIO(response.content)) as memfile:
                with memfile.open() as dataset:
                    data = dataset.read(1)
                    transform = dataset.transform
                    crs = dataset.crs
                    return data, transform, crs

        except Exception as e:
            raise Exception(f"Error loading GeoTIFF data: {str(e)}")

    def visualize_spatial(self, job_id):
        """Create spatial visualization"""
        try:
            data_url, job_info = self.get_job_data(job_id)
            
            process_graph = job_info.get('process', {}).get('process_graph', {})
            load_data = process_graph.get('load_data', {}).get('arguments', {})
            temporal_extent = load_data.get('temporal_extent', [])
            spatial_extent = load_data.get('spatial_extent', {})
            
            if not temporal_extent:
                raise Exception("No temporal extent found in job info")
            
            data, transform, crs = self.load_geotiff_data(data_url, temporal_extent)

            fig, ax = plt.subplots(figsize=(12, 8))
            
            extent = [
                spatial_extent.get('west', -180),
                spatial_extent.get('east', 180),
                spatial_extent.get('south', -90),
                spatial_extent.get('north', 90)
            ]

            im = ax.imshow(data, extent=extent, cmap='viridis')
            plt.colorbar(im, ax=ax, label='Value')
            
            timestamp = temporal_extent[0]
            ax.set_title(f'Spatial Distribution - Job {job_id}\nTimestamp: {timestamp}')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            ax.grid(True, linestyle='--', alpha=0.6)
            
            return fig

        except Exception as e:
            raise Exception(f"Error in spatial visualization: {str(e)}")

    def visualize_time_series(self, job_id):
        """Create time series visualization"""
        try:
            data_url, job_info = self.get_job_data(job_id)
            
            process_graph = job_info.get('process', {}).get('process_graph', {})
            load_data = process_graph.get('load_data', {}).get('arguments', {})
            temporal_extent = load_data.get('temporal_extent', [])
            spatial_extent = load_data.get('spatial_extent', {})
            
            if not temporal_extent:
                raise Exception("No temporal extent found in job info")
            
            timestamps, values = self.load_time_series_data(data_url, temporal_extent, spatial_extent)

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(timestamps, values, marker='o')
            
            ax.set_title(f'Time Series Analysis - Job {job_id}')
            ax.set_xlabel('Time')
            ax.set_ylabel('Mean Value')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            return fig

        except Exception as e:
            raise Exception(f"Error in time series visualization: {str(e)}")

@click.group()
def cli():
    """Visualisierungswerkzeug für OpenEO Job-Ergebnisse"""
    pass

@cli.command()
@click.argument('job_id')
@click.option('--type', type=click.Choice(['timeseries', 'spatial']), 
              default='spatial', help='Art der Visualisierung')
def visualize(job_id, type):
    """Visualisiere die Ergebnisse eines Jobs"""
    try:
        visualizer = DataVisualizer()
        
        if type == 'timeseries':
            visualizer.visualize_time_series(job_id)
        elif type == 'spatial':
            visualizer.visualize_spatial(job_id)
            
    except Exception as e:
        console.print(f"[red]Fehler bei der Visualisierung: {str(e)}[/red]")

if __name__ == '__main__':
    cli()