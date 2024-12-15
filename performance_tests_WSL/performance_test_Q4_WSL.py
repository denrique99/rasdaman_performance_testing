import requests
import json
import time
import numpy as np
from datetime import datetime
from rasdapy.db_connector import DBConnector
from rasdapy.query_executor import QueryExecutor

def test_openeo_performance():
    print("\nTesting OpenEO API...")

    body = {
        "title": "Q4",
        "description": "Q4",
        "process": {
            "process_graph": {
                "load_data": {
                    "process_id": "load_collection",
                    "arguments": {
                        "id": "era5_weekly",
                        "spatial_extent": {
                            "west": 6,
                            "east": 15,
                            "north": 55,
                            "south": 47
                        },
                        "temporal_extent": [
                            "1970-01-10T03:08:48.000Z",
                            "1970-01-10T03:11:34.000Z"
                        ]
                    }
                },
                "save": {
                    "process_id": "save_result",
                    "arguments": {
                        "x": {
                            "from_node": "load_data"
                        },
                        "format": "text/csv"
                    },
                    "result": True
                }
            }
        }
    }

    start_time = time.time()
    response = requests.post(
        "http://localhost:8080/rasdaman/openeo/result",
        json=body,
        auth=('rasadmin', 'rasadmin')
    )
    end_time = time.time()
    execution_time = end_time - start_time

    output_file_openeo = "openeo_resultQ4_WSL.csv"
    with open(output_file_openeo, 'wb') as file:
        file.write(response.content)

    return execution_time, "CSV Datei wurde gespeichert als 'openeo_resultQ4_WSL.csv'"

def test_wcs_performance():
    print("Testing WCS...")

    query = """for c in (era5_weekly) 
    return encode(c[Lat(47:55), 
    long(6:15), 
    ansi("1970-01-10T03:08:48.000Z":"1970-01-10T03:11:34.000Z")],"csv")"""

    params = {
        'SERVICE': 'WCS',
        'VERSION': '2.0.1',
        'REQUEST': 'ProcessCoverages',
        'QUERY': query
    }

    start_time = time.time()
    response = requests.post(
        "http://localhost:8080/rasdaman/ows",
        params=params,
        auth=('rasadmin', 'rasadmin')
    )
    end_time = time.time()
    execution_time = end_time - start_time

    output_file_wcs = "wcs_resultQ4_WSL.csv"
    with open(output_file_wcs, 'wb') as file:
        file.write(response.content)

    return execution_time, "CSV Datei wurde gespeichert als 'wcs_resultQ4_WSL.csv'"

def test_rasql_performance():
    print("Testing RasQL...")
    
    try:
        db_connector = DBConnector("localhost", 7001, "rasadmin", "rasadmin")
        query_executor = QueryExecutor(db_connector)
        db_connector.open()
        
        query = 'SELECT encode(era5_weekly[0:166,549:580,25:60], "csv") FROM era5_weekly'
        
        start_time = time.time()
        result = query_executor.execute_read(query)
        end_time = time.time()
        execution_time = end_time - start_time
        
        if result:
            data_array = result.to_array()
            reshaped_data = data_array.reshape(-1, data_array.shape[-1])
            np.savetxt('rasql_resultQ4_WSL.csv', reshaped_data, delimiter=',', fmt='%.8f')
            
        db_connector.close()
        return execution_time, "CSV Datei wurde gespeichert als 'rasql_resultQ4_WSL.csv'"
        
    except Exception as e:
        print(f"RasQL Error: {e}")
        return None, None

def main():
    print("Starting Performance Tests...\n")

    num_tests = 100
    results = []
    
    # Arrays to store times for statistical analysis
    openeo_times = []
    wcs_times = []
    rasql_times = []

    for i in range(num_tests):
        iteration_results = {
            'iteration': i + 1,
            'OpenEO': {'time': None, 'result': None},
            'WCS': {'time': None, 'result': None},
            'RasQL': {'time': None, 'result': None}
        }

        print(f"Iteration {i + 1}:")
        print("-" * 50)

        try:
            time, result = test_openeo_performance()
            if result:
                iteration_results['OpenEO'] = {'time': time, 'result': result}
                openeo_times.append(time)
                print(f"OpenEO Time: {time:.3f} seconds")
                print(f"OpenEO Result: {result}\n")
        except Exception as e:
            print(f"OpenEO Error: {e}\n")

        try:
            time, result = test_wcs_performance()
            if result:
                iteration_results['WCS'] = {'time': time, 'result': result}
                wcs_times.append(time)
                print(f"WCS Time: {time:.3f} seconds")
                print(f"WCS Result: {result}\n")
        except Exception as e:
            print(f"WCS Error: {e}\n")

        try:
            time, result = test_rasql_performance()
            if result:
                iteration_results['RasQL'] = {'time': time, 'result': result}
                rasql_times.append(time)
                print(f"RasQL Time: {time:.3f} seconds")
                print(f"RasQL Result: {result}\n")
        except Exception as e:
            print(f"RasQL Error: {e}\n")

        results.append(iteration_results)
        print("-" * 50)

    # Calculate and save detailed statistics
    with open('query_stats_Q4_WSL.txt', 'w') as f:
        f.write(f"Performance Test Results\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Number of Iterations: {num_tests}\n\n")

        for method, times in [('OpenEO', openeo_times), ('WCS', wcs_times), ('RasQL', rasql_times)]:
            if times:
                avg_time = np.mean(times)
                min_time = np.min(times)
                max_time = np.max(times)
                std_dev = np.std(times)
                
                print(f"\n{method} Statistics:")
                print(f"Average Time: {avg_time:.3f} seconds")
                print(f"Minimum Time: {min_time:.3f} seconds")
                print(f"Maximum Time: {max_time:.3f} seconds")
                print(f"Standard Deviation: {std_dev:.3f} seconds")
                
                f.write(f"\n{method} Statistics:\n")
                f.write(f"Average Time: {avg_time:.3f} seconds\n")
                f.write(f"Minimum Time: {min_time:.3f} seconds\n")
                f.write(f"Maximum Time: {max_time:.3f} seconds\n")
                f.write(f"Standard Deviation: {std_dev:.3f} seconds\n")
                f.write("\nIndividual Query Times:\n")
                for i, t in enumerate(times, 1):
                    f.write(f"Iteration {i}: {t:.3f} seconds\n")

if __name__ == "__main__":
    main()