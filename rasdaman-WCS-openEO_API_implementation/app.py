from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from lxml import etree
from datetime import datetime, timezone, timedelta
import time

app = Flask(__name__)
CORS(app)

# Rasdaman Konfiguration
RASDAMAN_URL = "http://localhost:8080/rasdaman/ows"
RASDAMAN_USER = "rasadmin"  
RASDAMAN_PASS = "rasadmin"

# Test-Prozessgraphen
user_process_graphs = {
    "ndvi_1": {
        "summary": "Normalized Difference Vegetation Index",
        "description": "Computes the NDVI from red and NIR bands",
        "process_graph": {
            "nir": {"process_id": "load_collection", "arguments": {"id": "S2_NIR"}},
            "red": {"process_id": "load_collection", "arguments": {"id": "S2_RED"}},
            "subtract": {"process_id": "subtract", "arguments": {"x": {"from_node": "nir"}, "y": {"from_node": "red"}}},
            "add": {"process_id": "add", "arguments": {"x": {"from_node": "nir"}, "y": {"from_node": "red"}}},
            "divide": {
                "process_id": "divide",
                "arguments": {"x": {"from_node": "subtract"}, "y": {"from_node": "add"}},
                "result": True
            }
        }
    }
}

def convert_hours_to_iso8601(hours_since_1900):
    """Konvertiert Stunden seit 1900-01-01 in ISO 8601-Datumsformat."""
    base_date = datetime(1900, 1, 1)
    target_date = base_date + timedelta(hours=hours_since_1900)
    return target_date.isoformat()

# Speicher für innerhalb der Session ertellte Jobs
jobs_store = {}

def get_rasdaman_collections():
    """Hole Collections von Rasdaman über WCS GetCapabilities"""
    try:
        # WCS GetCapabilities Request
        params = {
            'SERVICE': 'WCS',
            'VERSION': '2.0.1',
            'REQUEST': 'GetCapabilities'
        }
        
        print(f"Sending request to: {RASDAMAN_URL}")
        print(f"With parameters: {params}")
        
        # Request mit Basic Auth
        response = requests.get(
            RASDAMAN_URL, 
            params=params,
            auth=(RASDAMAN_USER, RASDAMAN_PASS)  # Basic Auth
        )
        
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            print("Successfully authenticated!")
            print(f"Response content: {response.text[:500]}...")  # First 500 chars
            
            # Parse XML Response
            root = etree.fromstring(response.content)
            
            # Define namespaces
            ns = {
                'wcs': 'http://www.opengis.net/wcs/2.0',
                'ows': 'http://www.opengis.net/ows/2.0'
            }
            
            # Extract collections
            collections = []
            coverages = root.xpath('//wcs:CoverageId', namespaces=ns)
            print(f"Found {len(coverages)} coverages")
            
            for coverage in coverages:
                collection_id = coverage.text
                print(f"Found coverage: {collection_id}")
                collections.append({
                    "stac_version": "1.0.0",
                    "id": collection_id,
                    "title": collection_id,
                    "description": f"Rasdaman coverage: {collection_id}",
                    "extent": {
                        "spatial": {
                            "bbox": [[-180, -90, 180, 90]]
                        },
                        "temporal": {
                            "interval": [[None, None]]
                        }
                    }
                })
            
            return collections
        else:
            print(f"Error: Rasdaman returned status code {response.status_code}")
            if response.status_code == 401:
                print("Authentication failed. Please check username and password.")
            return []
            
    except Exception as e:
        print(f"Error fetching collections: {e}")
        return []

# Endpunkt für Anzeige der verfügbaren Collections
@app.route('/collections')
def collections():
    """Liste verfügbare Collections"""
    collections_data = get_rasdaman_collections()
    
    return jsonify({
        "collections": collections_data,
        "links": []
    })

# Endpunkt für Anzeige der Metadaten einer bestimmten Collection
@app.route('/collections/<string:collection_id>')
def get_collection(collection_id):
    """Gibt Details zu einer bestimmten Collection zurück"""
    collection_metadata = get_collection_metadata(collection_id)
    if collection_metadata:
        return jsonify(collection_metadata)
    else:
        return jsonify({'error': f'Could not retrieve metadata for collection {collection_id}'}), 404

def get_collection_metadata(collection_id):
    try:
        params = {
            'SERVICE': 'WCS',
            'VERSION': '2.0.1',
            'REQUEST': 'DescribeCoverage',
            'COVERAGEID': collection_id
        }
        response = requests.get(
            RASDAMAN_URL,
            params=params,
            auth=(RASDAMAN_USER, RASDAMAN_PASS)
        )

        root = etree.fromstring(response.content)
        ns = {
            'wcs': 'http://www.opengis.net/wcs/2.0',
            'gml': 'http://www.opengis.net/gml/3.2',
            'gmlrgrid': 'http://www.opengis.net/gml/3.3/rgrid'
        }

        # Extraktion der Zeitgrenzen
        temporal_extent = None
        lower_corner = root.xpath('//gml:lowerCorner', namespaces=ns)
        upper_corner = root.xpath('//gml:upperCorner', namespaces=ns)
        if lower_corner and upper_corner:
            temporal_extent = [
                lower_corner[0].text.split()[0].strip('"'),
                upper_corner[0].text.split()[0].strip('"')
            ]

        # Extraktion der Zeitstempel
        time_values = []
        coefficients = root.xpath('//gmlrgrid:coefficients', namespaces=ns)
        if coefficients:
            raw_values = coefficients[0].text.split('"')
            time_values = [value for value in raw_values if value.strip()]

        return {
            "id": collection_id,
            "title": collection_id,
            "description": f"Rasdaman coverage: {collection_id}",
            "extent": {
                "spatial": {
                    "bbox": [[-180, -90, 180, 90]]
                },
                "temporal": {
                    "interval": [temporal_extent] if temporal_extent else [[None, None]]
                }
            },
            "stac_version": "1.0.0",
            "cube:dimensions": {
                "time": {
                    "type": "temporal",
                    "values": time_values
                },
                "x": {"type": "spatial", "axis": "x"},
                "y": {"type": "spatial", "axis": "y"}
            },
            "summaries": {}
        }
    except Exception as e:
        print(f"Error fetching collection metadata: {e}")
        return None

# Endpunkt für Anzeige der verfügbaren Datei-Formate
@app.route('/file_formats')
def file_formats():
    """Liste verfügbare Eingabe- und Ausgabeformate"""
    input_formats, output_formats = get_rasdaman_file_formats()

    return jsonify({
        "input": input_formats,
        "output": output_formats
    })

def get_rasdaman_file_formats():
    """Hole die von Rasdaman unterstützten Dateiformate"""
    try:
        # WCS GetCapabilities Request
        params = {
            'SERVICE': 'WCS',
            'VERSION': '2.0.1',
            'REQUEST': 'GetCapabilities'
        }

        print(f"Sending request to: {RASDAMAN_URL}")
        print(f"With parameters: {params}")

        # Request mit Basic Auth
        response = requests.get(
            RASDAMAN_URL,
            params=params,
            auth=(RASDAMAN_USER, RASDAMAN_PASS)
        )

        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")

        if response.status_code == 200:
            print("Successfully retrieved file format information!")

            # Parse XML Response
            root = etree.fromstring(response.content)

            # Define namespaces
            ns = {
                'wcs': 'http://www.opengis.net/wcs/2.0',
                'ows': 'http://www.opengis.net/ows/2.0'
            }

            # Extract input and output formats
            input_formats = {}
            output_formats = {}

            # Extracting input formats
            for format_element in root.xpath('//wcs:ServiceMetadata/wcs:formatSupported', namespaces=ns):
                format_name = format_element.text
                input_formats[format_name] = {
                    "title": format_name
                }

            # Extracting output formats
            for format_element in root.xpath('//wcs:ServiceMetadata/wcs:formatSupported', namespaces=ns):
                format_name = format_element.text
                output_formats[format_name] = {
                    "title": format_name
                }

            return input_formats, output_formats
        else:
            print(f"Error: Rasdaman returned status code {response.status_code}")
            if response.status_code == 401:
                print("Authentication failed. Please check username and password.")
            return {}, {}

    except Exception as e:
        print(f"Error fetching file formats: {e}")
        return {}, {}

# Endpunkt für Anzeige der verfügbaren Prozesse
@app.route('/processes')
def get_processes():
    """Liste verfügbare Prozesse"""
    processes = get_rasdaman_processes()
    
    return jsonify({
        "processes": processes,
        "links": []
    })

def get_rasdaman_processes():
    """Hole die von Rasdaman unterstützten Prozesse"""
    try:
        # WCS GetCapabilities Request
        params = {
            'SERVICE': 'WCS',
            'VERSION': '2.0.1',
            'REQUEST': 'GetCapabilities'
        }

        print(f"Sending request to: {RASDAMAN_URL}")
        print(f"With parameters: {params}")

        # Request mit Basic Auth
        response = requests.get(
            RASDAMAN_URL,
            params=params,
            auth=(RASDAMAN_USER, RASDAMAN_PASS)
        )

        print(f"Response status code: {response.status_code}")
        print(f"Response content: {response.text}")

        if response.status_code == 200:
            print("Successfully retrieved process information!")

            # Parse XML Response
            root = etree.fromstring(response.content)

            # Define namespaces
            ns = {
                'wcs': 'http://www.opengis.net/wcs/2.0',
                'ows': 'http://www.opengis.net/ows/2.0'
            }

            # Extract processes
            processes = []

            for process_element in root.xpath('//ows:Operation', namespaces=ns):
                process_name = process_element.get('name')
                
                # Überprüfe, ob es sich um einen unterstützten Prozess handelt
                if process_name in ['GetCoverage', 'DescribeCoverage', 'ProcessCoverages']:
                    process_metadata = {
                        "id": process_name,
                        "summary": process_name,
                        "description": process_name,
                        "parameters": [],
                        "returns": {
                            "description": "Processed data",
                            "schema": {}
                        },
                        "categories": [],
                        "deprecated": False,
                        "experimental": False,
                        "exceptions": {},
                        "examples": [],
                        "links": []
                    }
                    processes.append(process_metadata)

            return processes
        else:
            print(f"Error: Rasdaman returned status code {response.status_code}")
            if response.status_code == 401:
                print("Authentication failed. Please check username and password.")
            return []

    except Exception as e:
        print(f"Error fetching processes: {e}")
        return []

# Endpunkt für Anzeige/Erstellung von Prozess-Graphen
@app.route('/process_graphs', methods=['GET', 'POST'])
def process_graphs_endpoint():
    if request.method == 'GET':
        processes = []
        for pg_id, pg_data in user_process_graphs.items():
            process = {
                "id": pg_id,
                "summary": pg_data.get("summary", ""),
                "description": pg_data.get("description", ""),
                "categories": pg_data.get("categories", []),
                "parameters": pg_data.get("parameters", []),
                "returns": pg_data.get("returns", {}),
                "deprecated": pg_data.get("deprecated", False),
                "experimental": pg_data.get("experimental", False),
                "examples": pg_data.get("examples", []),
                "links": []
            }
            processes.append(process)
            
        return jsonify({
            "processes": processes,
            "links": [
                {
                    "rel": "self",
                    "href": request.base_url,
                    "type": "application/json"
                }
            ]
        })
    elif request.method == 'POST':
        process_graph_data = request.get_json()
        new_process_graph = save_process_graph(process_graph_data)
        return jsonify(new_process_graph), 201

def get_user_process_graphs():
    """Hole alle benutzerdefinierten Prozessgraphen des Nutzers"""
    # statische Liste
    return list(user_process_graphs.values())

# Endpunkt für Anzeige dvon Details eines Prozess-Graphens
@app.route('/process_graphs/<string:process_graph_id>', methods=['GET'])
def get_process_graph(process_graph_id):
    """Hole Details eines bestimmten Prozessgraphen"""
    process_graph = get_process_graph_details(process_graph_id)
    if process_graph:
        return jsonify(process_graph)
    else:
        return jsonify({'error': f'Could not retrieve process graph {process_graph_id}'}), 404

def get_process_graph_details(process_graph_id):
    """Hole die Details eines bestimmten Prozessgraphen"""
    if process_graph_id in user_process_graphs:
        process_graph = user_process_graphs[process_graph_id]
        return process_graph
    else:
        return None

def save_process_graph(process_graph_data):
    # Generiere eine eindeutige ID für den neuen Prozessgraphen
    new_id = f"pg-{len(user_process_graphs) + 1}"
    
    # Füge den neuen Prozessgraphen zur Datenstruktur hinzu
    user_process_graphs[new_id] = process_graph_data
    process_graph_data["id"] = new_id
    
    return process_graph_data

# Endpunkt für das Aktualisieren eines bestehenden Prozessgraphen
@app.route('/process_graphs/<string:process_graph_id>', methods=['PATCH'])
def update_process_graph(process_graph_id):
    process_graph_data = request.get_json()
    updated_process_graph = update_process_graph(process_graph_id, process_graph_data)
    if updated_process_graph:
        return jsonify(updated_process_graph)
    else:
        return jsonify({'error': f'Process graph {process_graph_id} not found'}), 404

def update_process_graph(process_graph_id, process_graph_data):
    if process_graph_id in user_process_graphs:
        user_process_graphs[process_graph_id].update(process_graph_data)
        return user_process_graphs[process_graph_id]
    else:
        return None

# Endpunkt für das Löschen eines Prozessgraphen
@app.route('/process_graphs/<string:process_graph_id>', methods=['DELETE'])
def delete_process_graph(process_graph_id):
    if delete_process_graph(process_graph_id):
        return '', 204
    else:
        return jsonify({'error': f'Process graph {process_graph_id} not found'}), 404

def delete_process_graph(process_graph_id):
    if process_graph_id in user_process_graphs:
        del user_process_graphs[process_graph_id]
        return True
    else:
        return False

# Endpunkt für Anzeige der verfügbaren OpenEO Funktionen
@app.route('/')
def get_capabilities():
    return jsonify({
        "api_version": "1.2.0",
        "backend_version": "1.0.0",
        "stac_version": "1.0.0",
        "type": "Catalog",
        "id": "rasdaman-openeo",
        "title": "Rasdaman OpenEO Backend",
        "description": "OpenEO API implementation for Rasdaman",
        "production": False,
        "endpoints": [
            {
                "path": "/collections",
                "methods": ["GET"]
            },
            {
                "path": "/processes",
                "methods": ["GET"]
            },
            {
                "path": "/jobs",
                "methods": ["GET", "POST"]
            },
            {
                "path": "/file_formats",
                "methods": ["GET"]
            },
            {
                "path": "/process_graphs",
                "methods": ["GET", "POST", "PATCH", "DELETE"]
            }
        ],
        "links": [
            {
                "rel": "self",
                "href": request.base_url,
                "type": "application/json"
            }
        ]
    })

# Endpunkt für Anzeige der OpenEO Version
@app.route('/.well-known/openeo')
def get_openeo_versions():
    return jsonify({
        "versions": [
            {
                "url": "http://localhost:5000",
                "api_version": "1.2.0",
                "production": False
            }
        ]
    })

# Endpunkt für das globale Job-Handling
@app.route('/jobs', methods=['GET', 'POST'])
def jobs():
    if request.method == 'GET':
        return jsonify({
            "jobs": list(jobs_store.values()),
            "links": [{
                "rel": "self",
                "href": request.base_url,
                "type": "application/json"
            }]
        })
    
    elif request.method == 'POST':
        job_data = request.get_json()
        job_id = f"job-{len(jobs_store) + 1}"

        collection_id = job_data["process"]["process_graph"]["load_data"]["arguments"]["id"]

        new_job = {
            "id": job_id,
            "title": job_data.get("title"),
            "description": job_data.get("description"),
            "process": job_data.get("process"),
            "status": "created",
            "created": datetime.now().isoformat() + "Z",
            "plan": job_data.get("plan", "free"),
            "budget": job_data.get("budget", None),
            "log_level": job_data.get("log_level", "info"),
            "collection_id": collection_id
        }
        
        jobs_store[job_id] = new_job
        
        response = jsonify(new_job)
        response.headers["Location"] = f"{request.base_url}/{job_id}"
        response.headers["OpenEO-Identifier"] = job_id
        return response, 201

# Endpunkt für das individuelle Job-Handling
@app.route('/jobs/<string:job_id>', methods=['GET', 'PATCH', 'DELETE'])
def job_details(job_id):
    if request.method == 'GET':
        job = jobs_store.get(job_id)
        if job:
            return jsonify(job)
        else:
            return jsonify({'error': f'Job {job_id} not found'}), 404
    
    elif request.method == 'PATCH':
        job_updates = request.get_json()
        updated_job = update_job(job_id, job_updates)
        if updated_job:
            return jsonify(updated_job)
        else:
            return jsonify({'error': f'Job {job_id} not found'}), 404
    
    elif request.method == 'DELETE':
        if delete_job(job_id):
            return '', 204
        else:
            return jsonify({'error': f'Job {job_id} not found'}), 404

def update_job(job_id, job_updates):
    if job_id in jobs_store:
        job = jobs_store[job_id]
        job.update(job_updates)
        return job
    else:
        return None

def delete_job(job_id):
    if job_id in jobs_store:
        del jobs_store[job_id]
        return True
    else:
        return False

def parse_temporal_interval(interval: str) -> tuple:
    """Parse temporal interval in ISO 8601 format."""
    start, end = interval.split('/')
    return datetime.fromisoformat(start), datetime.fromisoformat(end)

def construct_wcps_query(collection_id: str, temporal_extent=None, spatial_extent=None):
    query = f"for data in ({collection_id})"
    
    if temporal_extent:
        start, end = parse_temporal_interval(temporal_extent)
        query += f" where time(data) >= '{start.isoformat()}' and time(data) <= '{end.isoformat()}'"
    
    if spatial_extent:
        west, south, east, north = spatial_extent
        query += f" where Long(data) >= {west} and Long(data) <= {east}"
        query += f" and Lat(data) >= {south} and Lat(data) <= {north}"
    
    query += " return encode(data, 'GTiff')"
    return query

# Endpunkt für die Job-Ausführung
@app.route('/jobs/<job_id>/results', methods=['POST'])
def start_job(job_id):
    if job_id not in jobs_store:
        return jsonify({"error": f"Job {job_id} not found"}), 404
        
    job = jobs_store[job_id]
    
    try:
        # Starte die Zeitmessung
        start_time = time.time()  # Zeitpunkt vor der Ausführung des Jobs

        job['status'] = 'running'
        
        # Extrahiere die räumlichen und zeitlichen Parameter
        arguments = job["process"]["process_graph"]["load_data"]["arguments"]
        spatial_extent = arguments["spatial_extent"]
        temporal_extent = arguments["temporal_extent"]
        
        # Konvertiere den Zeitbereich ins ISO 8601-Format
        start_time_str = datetime.fromisoformat(temporal_extent[0]).astimezone(timezone.utc).isoformat()
        end_time_str = datetime.fromisoformat(temporal_extent[1]).astimezone(timezone.utc).isoformat()
        
        # WCS-Anfrage vorbereiten
        params = {
            'SERVICE': 'WCS',
            'VERSION': '2.0.1',
            'REQUEST': 'GetCoverage',
            'COVERAGEID': job["collection_id"],
            'SUBSET': [
                f'Lat({spatial_extent["south"]},{spatial_extent["north"]})',
                f'Long({spatial_extent["west"]},{spatial_extent["east"]})',
                f'ansi("{start_time_str}","{end_time_str}")'
            ],
            'FORMAT': 'application/json'
        }
        
        # API-Anfrage an Rasdaman
        response = requests.get(
            RASDAMAN_URL,
            params=params,
            auth=(RASDAMAN_USER, RASDAMAN_PASS),
            stream=True
        )
        
        # Berechne die verstrichene Zeit
        elapsed_time = time.time() - start_time  # Zeit in Sekunden
        print(f"Job {job_id} completed in {elapsed_time:.2f} seconds")
        
        # Verarbeite die Antwort
        if response.status_code == 200:
            result = response.json()  # Wenn JSON-Format erwartet wird
            
            # Job erfolgreich abgeschlossen
            job['status'] = 'finished'
            job['result'] = {'data': result}
        else:
            # Fehler vom WCS-Server
            job['status'] = 'error'
            job['error'] = response.text
        
        # Füge die verstrichene Zeit zu den Ergebnissen hinzu
        job['execution_time'] = f"{elapsed_time:.2f} seconds"
        
        return jsonify(job), 202
        
    except Exception as e:
        print(f"Error executing job: {str(e)}")
        job['status'] = 'error'
        job['error'] = str(e)
        return jsonify({"error": str(e)}), 500

# Endpunkt für die Anzeige der Ergebnisse eines fertigen Jobs
@app.route('/jobs/<job_id>/results', methods=['GET'])
def get_job_results(job_id):
    """Hole die Ergebnisse eines fertiggestellten Jobs"""
    if job_id not in jobs_store:
        return jsonify({"error": f"Job {job_id} not found"}), 404
        
    job = jobs_store[job_id]
    
    if job['status'] != 'finished':
        return jsonify({"error": "Job is not finished yet"}), 400
        
    try:
        # Result metadata im STAC-Format
        result = {
            "stac_version": "1.0.0",
            "id": job_id,
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    float(job['process']['process_graph']['load_data']['arguments']['spatial_extent']['west']),
                    float(job['process']['process_graph']['load_data']['arguments']['spatial_extent']['north'])
                ]
            },
            "properties": {
                "datetime": job['process']['process_graph']['load_data']['arguments']['temporal_extent'][0],
                "title": job.get('title', ''),
                "description": job.get('description', ''),
                "temperature": job.get('result', {}).get('temperature')
            },
            "assets": {
                "data": {
                    "href": f"http://localhost:8080/rasdaman/ows?service=WCS&version=2.0.1&request=GetCoverage&coverageId=era5_weekly",
                    "type": "image/tiff",
                    "roles": ["data"]
                },
                "metadata": {
                    "href": f"http://localhost:5000/jobs/{job_id}",
                    "type": "application/json",
                    "roles": ["metadata"]
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"http://localhost:5000/jobs/{job_id}/results",
                    "type": "application/json"
                }
            ]
        }
        
        return jsonify(result)
        
    except Exception as e:
        print(f"Error executing job: {str(e)}")
        job['status'] = 'error'
        job['error'] = {
            'code': 'InternalServerError',
            'message': str(e),
            'status_code': 500
        }
        return jsonify(job['error']), job['error']['status_code']

# Debug-Modus (automatischer Reload der app.py nach Änderung)
# if __name__ == '__main__':
#     app.run(debug=True, port=5000)

if __name__ == '__main__':
    app.run(debug=False, port=5000, use_reloader=False)