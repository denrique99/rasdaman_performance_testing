import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
from interface import OpenEOClient, validate_spatial_extent, validate_temporal_extent
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from datetime import datetime, timedelta

console = Console()

@click.group()
@click.option('--url', default='http://localhost:5000', help='OpenEO API URL')
@click.pass_context
def cli(ctx, url):
    """Rasdaman OpenEO CLI Interface"""
    ctx.obj = OpenEOClient(url)

@cli.command()
@click.pass_obj
def list_collections(client):
    """Liste alle verfügbaren Collections"""
    collections = client.make_request('collections')
    
    if collections:
        table = Table(title="Available Collections")
        table.add_column("ID", style="cyan")
        table.add_column("Title", style="green")
        table.add_column("Description", style="yellow")
        
        for collection in collections.get('collections', []):
            table.add_row(
                collection.get('id', ''),
                collection.get('title', ''),
                collection.get('description', '')
            )
        
        console.print(table)

@cli.command()
@click.pass_obj
@click.argument('collection_id')
def describe_collection(client, collection_id):
    """Zeige Details einer Collection"""
    details = client.make_request(f'collections/{collection_id}')
    if details:
        console.print(Panel.fit(
            json.dumps(details, indent=2),
            title=f"Collection: {collection_id}",
            border_style="green"
        ))

@cli.command()
@click.pass_obj
def list_processes(client):
    """Liste alle verfügbaren Prozesse"""
    processes = client.make_request('processes')
    
    if processes:
        table = Table(title="Available Processes")
        table.add_column("ID", style="cyan")
        table.add_column("Summary", style="green")
        
        for process in processes.get('processes', []):
            table.add_row(
                process.get('id', ''),
                process.get('summary', '')
            )
        
        console.print(table)

@cli.command()
@click.pass_obj
def list_jobs(client):
    """Liste alle Jobs"""
    jobs = client.make_request('jobs')
    
    if jobs:
        table = Table(title="Jobs")
        table.add_column("ID", style="cyan")
        table.add_column("Title", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Created", style="blue")
        table.add_column("Execution Time", style="magenta")
        
        for job in jobs.get('jobs', []):
            table.add_row(
                job.get('id', ''),
                job.get('title', ''),
                job.get('status', ''),
                job.get('created', ''),
                job.get('execution_time', 'N/A')
            )
        
        console.print(table)

def display_timestamps_paginated(timestamps, page_size=20):
    """Zeigt Zeitstempel seitenweise an"""
    total_pages = (len(timestamps) + page_size - 1) // page_size
    current_page = 1

    while True:
        console.clear()
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, len(timestamps))
        
        console.print(f"\n[yellow]Zeitpunkte (Seite {current_page}/{total_pages}):[/yellow]")
        for i, ts in enumerate(timestamps[start_idx:end_idx], start_idx + 1):
            console.print(f"{i}. {ts}")
        
        console.print("\n[blue]Navigation:[/blue]")
        console.print("n: Nächste Seite")
        console.print("p: Vorherige Seite")
        console.print("g: Gehe zu Seite")
        console.print("s: Zeitpunkt auswählen")
        console.print("q: Abbrechen")
        
        choice = click.prompt(
            'Wählen Sie eine Option',
            type=click.Choice(['n', 'p', 'g', 's', 'q'])
        )
        
        if choice == 'n' and current_page < total_pages:
            current_page += 1
        elif choice == 'p' and current_page > 1:
            current_page -= 1
        elif choice == 'g':
            page = click.prompt(
                f'Gehe zu Seite (1-{total_pages})',
                type=click.IntRange(1, total_pages)
            )
            current_page = page
        elif choice == 's':
            idx = click.prompt(
                f'Wählen Sie einen Zeitpunkt (1-{len(timestamps)})',
                type=click.IntRange(1, len(timestamps))
            )
            return idx - 1
        elif choice == 'q':
            return None

@cli.command()
@click.pass_obj
@click.option('--title', prompt='Job title', help='Title for the job')
@click.option('--collection', prompt='Collection ID', help='Collection ID to process')
def create_job(client, title, collection):
    """Erstelle einen neuen Job mit präziser Zeitauswahl"""
    try:
        # Hole Collection-Details
        collection_info = client.get_collection_details(collection)
        if not collection_info:
            console.print("[red]Collection nicht gefunden[/red]")
            return

        # Extrahiere verfügbare Zeitstempel
        timestamps = collection_info.get('cube:dimensions', {}).get('time', {}).get('values', [])
        if not timestamps:
            console.print("[red]Keine Zeitinformationen in der Collection gefunden[/red]")
            return

        # Zeige Zeitbereichsinformationen
        console.print(f"\n[green]Verfügbarer Zeitbereich für {collection}:[/green]")
        console.print(f"Start: {timestamps[0]}")
        console.print(f"Ende: {timestamps[-1]}")
        console.print(f"Anzahl Zeitpunkte: {len(timestamps)}")
        console.print(f"Intervall: 1 Sekunde")

        # Zeitbereichsauswahl
        time_selection = click.prompt(
            '\nZeitauswahl',
            type=click.Choice(['1', '2']),
            prompt_suffix='\n1. Einzelner Zeitpunkt\n2. Zeitbereich\nIhre Wahl: '
        )

        if time_selection == '1':
            # Einzelner Zeitpunkt
            console.print("\n[green]Zeitpunkt auswählen:[/green]")
            idx = display_timestamps_paginated(timestamps)
            if idx is None:
                console.print("[yellow]Auswahl abgebrochen[/yellow]")
                return
            temporal_extent = [timestamps[idx], timestamps[idx]]
        else:
            # Zeitbereich
            console.print("\n[green]Startzeit auswählen:[/green]")
            start_idx = display_timestamps_paginated(timestamps)
            if start_idx is None:
                console.print("[yellow]Auswahl abgebrochen[/yellow]")
                return

            console.print("\n[green]Endzeit auswählen:[/green]")
            # Zeige nur Zeitstempel nach dem Startzeitpunkt
            end_timestamps = timestamps[start_idx:]
            end_idx = display_timestamps_paginated(end_timestamps)
            if end_idx is None:
                console.print("[yellow]Auswahl abgebrochen[/yellow]")
                return
            
            # Korrigiere den End-Index relativ zum Gesamtarray
            end_idx = start_idx + end_idx

            temporal_extent = [timestamps[start_idx], timestamps[end_idx]]

        # Räumliche Ausdehnung
        spatial_bounds = collection_info.get('extent', {}).get('spatial', {}).get('bbox', [[-180, -90, 180, 90]])[0]
        
        console.print("\n[green]Räumliche Ausdehnung:[/green]")
        console.print(f"Verfügbare Grenzen: W={spatial_bounds[0]}, S={spatial_bounds[1]}, E={spatial_bounds[2]}, N={spatial_bounds[3]}")
        
        spatial_extent = {
            "west": click.prompt('West coordinate', type=float, default=9.9),
            "east": click.prompt('East coordinate', type=float, default=10.1),
            "north": click.prompt('North coordinate', type=float, default=51.1),
            "south": click.prompt('South coordinate', type=float, default=50.9)
        }

        # Job-Konfiguration
        job_data = {
            "title": title,
            "process": {
                "process_graph": {
                    "load_data": {
                        "process_id": "load_collection",
                        "arguments": {
                            "id": collection,
                            "spatial_extent": spatial_extent,
                            "temporal_extent": temporal_extent
                        }
                    }
                }
            }
        }

        # Zeige Zusammenfassung
        console.print("\n[yellow]Job-Details zur Bestätigung:[/yellow]")
        console.print(Panel.fit(
            f"""
Collection: {collection}
Zeitbereich: 
  Von: {temporal_extent[0]}
  Bis: {temporal_extent[1]}
Räumliche Ausdehnung:
  West:  {spatial_extent['west']}
  East:  {spatial_extent['east']}
  North: {spatial_extent['north']}
  South: {spatial_extent['south']}
            """,
            title="Job-Konfiguration",
            border_style="yellow"
        ))

        if click.confirm('\nMöchten Sie den Job mit diesen Einstellungen erstellen?'):
            response = client.make_request('jobs', method='POST', data=job_data)
            if response:
                console.print(Panel.fit(
                    json.dumps(response, indent=2),
                    title="Job Created",
                    border_style="green"
                ))

                console.print(f"\n[green]Job erfolgreich erstellt![/green]")
                console.print(f"Job ID: {response.get('id')}")
                console.print("Verwende 'start-job <job-id>' um den Job zu starten")
        else:
            console.print("[yellow]Job-Erstellung abgebrochen[/yellow]")

    except Exception as e:
        console.print(f"[red]Fehler beim Erstellen des Jobs: {str(e)}[/red]")
        import traceback
        console.print(Panel.fit(traceback.format_exc(), title="Error Details", border_style="red"))

@cli.command()
@click.pass_obj
@click.argument('job_id')
def start_job(client, job_id):
    """Starte einen Job"""
    response = client.make_request(f'jobs/{job_id}/results', method='POST')
    if response:
        console.print(Panel.fit(
            json.dumps(response, indent=2),
            title=f"Job {job_id} Started",
            border_style="green"
        ))

@cli.command()
@click.pass_obj
@click.argument('job_id')
def get_results(client, job_id):
    """Hole die Ergebnisse eines Jobs"""
    results = client.make_request(f'jobs/{job_id}/results')
    if results:
        console.print(Panel.fit(
            json.dumps(results, indent=2),
            title=f"Results for Job {job_id}",
            border_style="green"
        ))

@cli.command()
@click.pass_obj
@click.argument('job_id')
def delete_job(client, job_id):
    """Lösche einen Job"""
    response = client.make_request(f'jobs/{job_id}', method='DELETE')
    if response is not None:
        console.print(f"[green]Job {job_id} successfully deleted[/green]")

if __name__ == '__main__':
    cli()