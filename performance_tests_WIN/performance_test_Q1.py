
import requests
import json
import subprocess
import time
import re

def test_openeo_performance():
    print("\nTesting OpenEO API...")

    body = {
        "title": "Q1 - 2m Temperature",
        "description": "Get 2m temperature for a specific location and point in time",
        "process": {
            "process_graph": {
                "load_data": {
                    "process_id": "load_collection",
                    "arguments": {
                        "id": "era5_weekly",
                        "spatial_extent": {
                            "west": 11.6211,
                            "east": 11.6411,
                            "north": 52.1440,
                            "south": 52.1240
                        },
                        "temporal_extent": [
                            "1970-01-10T03:08:48.000Z",
                            "1970-01-10T03:08:48.000Z"
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

    # Speichern der OpenEO-Antwort als CSV-Datei
    output_file_openeo = "openeo_result.csv"
    with open(output_file_openeo, 'wb') as file:
        file.write(response.content)

    return execution_time, "CSV Datei wurde gespeichert als 'openeo_result.csv'"

def test_wcs_performance():
    print("Testing WCS...")

    query = """for c in (era5_weekly) 
    return encode(c[Lat(52.1240:52.1440), 
    Long(11.6211:11.6411), 
    ansi("1970-01-10T03:08:48.000Z")], "csv")"""

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

    # Speichern der WCS-Antwort als CSV-Datei
    output_file_wcs = "wcs_result.csv"
    with open(output_file_wcs, 'wb') as file:
        file.write(response.content)

    return execution_time, "CSV Datei wurde gespeichert als 'wcs_result.csv'"

def test_rasql_performance():
    print("Testing RasQL...")

# # Query für numerisches Ergebnis
#     # # Der vollständige Befehl zum Ausführen von rasql im WSL (Ubuntu-Umgebung)
#     # rasql_cmd = [
#     #     "wsl",  # Aufruf des WSL-Subsystems
#     #     "/opt/rasdaman/bin/rasql",  # Vollständiger Pfad zur rasql-Binärdatei unter WSL
#     #     "-q", 'SELECT encode(era5_weekly[0,568,46] * 10, "csv") FROM era5_weekly',
#     #     "--user", "rasadmin",
#     #     "--passwd", "rasadmin",
#     #     "--out", "string"
#     # ]

    # Pfad für die CSV-Ausgabe im WSL-kompatiblen Format
    output_file = "/mnt/c/Users/Analyst05/OneDrive - The Analysts GmbH/Dokumente/Python Programme/Performance Tests/rasql_output"

    # RasQL-Befehl zum Ausführen im WSL-Subsystem
    rasql_cmd = [
        "wsl",  # Aufruf des WSL-Subsystems
        "/opt/rasdaman/bin/rasql",  # RasQL-Binärdatei unter WSL
        "-q", 'SELECT encode(era5_weekly[0,568,46], "csv") FROM era5_weekly',  # Abfrage
        "--user", "rasadmin",
        "--passwd", "rasadmin",
        "--out", "file",  # Ausgabe im CSV-Format
        "--outfile", output_file  # Ausgabe an den angegebenen Pfad
    ]

    start_time = time.time()
    try:
        # Ausführen des RasQL-Befehls im WSL-Subsystem
        process = subprocess.run(rasql_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        if process.returncode == 0:
            end_time = time.time()
            execution_time = end_time - start_time
            return execution_time, "CSV Datei wurde gespeichert als 'rasql_output.csv'"
        else:
            # Wenn der Befehl fehlschlägt, gebe den Fehler aus
            print(f"RasQL command failed with error: {process.stderr}")
            return None, None

    except Exception as e:
        # Wenn es zu einer Ausnahme kommt, gebe die Fehlermeldung aus
        print(f"Error executing RasQL: {e}")
        return None, None

def main():
    print("Starting Performance Tests...\n")

    num_tests = 100
    results = []

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
                print(f"OpenEO Time: {time:.3f} seconds")
                print(f"OpenEO Result: {result}\n")
        except Exception as e:
            print(f"OpenEO Error: {e}\n")

        try:
            time, result = test_wcs_performance()
            if result:
                iteration_results['WCS'] = {'time': time, 'result': result}
                print(f"WCS Time: {time:.3f} seconds")
                print(f"WCS Result: {result}\n")
        except Exception as e:
            print(f"WCS Error: {e}\n")

        try:
            time, result = test_rasql_performance()
            if result:
                iteration_results['RasQL'] = {'time': time, 'result': result}
                print(f"RasQL Time: {time:.3f} seconds")
                print(f"RasQL Result: {result}\n")
        except Exception as e:
            print(f"RasQL Error: {e}\n")

        results.append(iteration_results)
        print("-" * 50)

    # Calculate averages only for successful queries
    print("\nAverage Times for Successful Queries:")
    for method in ['OpenEO', 'WCS', 'RasQL']:
        times = [r[method]['time'] for r in results if r[method]['time'] is not None]
        if times:
            avg_time = sum(times) / len(times)
            print(f"{method} Average Time: {avg_time:.3f} seconds")

if __name__ == "__main__":
    main()

