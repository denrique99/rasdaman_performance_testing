import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from interface import OpenEOClient
import streamlit as st
from datetime import datetime
import pandas as pd
import plotly.express as px
import time
from visualize_data import DataVisualizer
import matplotlib.pyplot as plt

def show_jobs():
    st.header("Jobs")
    client = OpenEOClient()
    
    # Auto-refresh using session state
    if 'refresh_counter' not in st.session_state:
        st.session_state.refresh_counter = 0
        
    auto_refresh = st.sidebar.checkbox("Auto-refresh")
    if auto_refresh:
        st.session_state.refresh_counter += 1
        st.experimental_rerun()
    
    if st.button("Refresh Jobs"):
        st.session_state.refresh_counter += 1
        st.experimental_rerun()
    
    with st.spinner("Loading jobs..."):
        jobs = client.make_request('jobs')
    
    if jobs:
        df = pd.DataFrame(jobs.get('jobs', []))
        if not df.empty:
            styled_df = df.style.applymap(
                lambda x: f"color: {'created': 'blue', 'running': 'orange', 'finished': 'green', 'error': 'red'}.get(x, 'black')",
                subset=['status']
            )
            
            st.dataframe(styled_df)
            
            if not df.empty:
                selected_job = st.selectbox(
                    "Select Job",
                    df['id'].tolist()
                )
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("Start Job"):
                        with st.spinner("Starting job..."):
                            response = client.make_request(
                                f"jobs/{selected_job}/results", 
                                method='POST'
                            )
                            if response:
                                st.success(f"Job {selected_job} started!")
                                st.json(response)
                
                with col2:
                    if st.button("Get Results"):
                        with st.spinner("Fetching results..."):
                            # First check job status
                            job_status = client.make_request(f"jobs/{selected_job}")
                            if job_status and job_status.get('status') == 'finished':
                                results = client.make_request(f"jobs/{selected_job}/results")
                                if results:
                                    st.success("Results retrieved successfully")
                                    st.json(results)
                            else:
                                st.warning("Job is not finished yet. Current status: " + 
                                         job_status.get('status', 'unknown'))
                
                with col3:
                    if st.button("Delete Job"):
                        confirm = st.checkbox("Confirm deletion")
                        if confirm:
                            with st.spinner("Deleting job..."):
                                response = client.make_request(
                                    f"jobs/{selected_job}", 
                                    method='DELETE'
                                )
                                if response is not None:
                                    st.success(f"Job {selected_job} deleted!")
                                    st.experimental_rerun()
        else:
            st.info("No jobs available")
    else:
        st.error("Could not fetch jobs")

def create_time_selection_section(time_values, collection_id):
    st.subheader("Temporal Extent")
    
    # Start time selection with filtering
    start_filter = st.text_input("Filter start times", key="start_filter", placeholder="YYYY-MM-DD")
    filtered_starts = [t for t in time_values if start_filter.lower() in t.lower()]
    start_time = st.selectbox("Select Start Time", filtered_starts, key="start_time_select")
    
    # End time selection with filtering (including start_time)
    if start_time:
        valid_end_times = [t for t in time_values if t >= start_time]
        end_filter = st.text_input("Filter end times", key="end_filter", placeholder="YYYY-MM-DD")
        filtered_ends = [t for t in valid_end_times if end_filter.lower() in t.lower()]
        end_time = st.selectbox("Select End Time", filtered_ends, key="end_time_select")
        
        if end_time:
            st.info(f"Selected range: {start_time} to {end_time}")
            return start_time, end_time
    
    return None, None

def create_job_section():
    st.header("Create New Job")
    client = OpenEOClient()
    collections = client.make_request('collections')
    
    if collections:
        collection_ids = [c.get('id') for c in collections.get('collections', [])]
        
        with st.form("create_job"):
            title = st.text_input("Job Title")
            collection_id = st.selectbox("Collection", collection_ids)
            
            if collection_id:
                collection_details = client.make_request(f'collections/{collection_id}')
                if collection_details and 'cube:dimensions' in collection_details:
                    time_values = collection_details['cube:dimensions'].get('time', {}).get('values', [])
                    
                    if time_values:
                        start_time, end_time = create_time_selection_section(time_values, collection_id)
                    else:
                        st.warning("No time values available for this collection")
                        start_time, end_time = None, None
                        
            st.subheader("Spatial Extent")
            col1, col2 = st.columns(2)
            with col1:
                west = st.number_input("West", value=0.0)
                east = st.number_input("East", value=10.0)
            with col2:
                north = st.number_input("North", value=50.0)
                south = st.number_input("South", value=40.0)
            
            submit = st.form_submit_button("Create Job")
            if submit:
                if not title:
                    st.error("Please enter a job title")
                    return
                    
                if not (start_time and end_time):
                    st.error("Please select temporal extent")
                    return
                   
                job_data = {
                    "title": title,
                    "process": {
                        "process_graph": {
                            "load_data": {
                                "process_id": "load_collection",
                                "arguments": {
                                    "id": collection_id,
                                    "spatial_extent": {
                                        "west": west,
                                        "east": east,
                                        "north": north,
                                        "south": south
                                    },
                                    "temporal_extent": [start_time, end_time]
                                }
                            }
                        }
                    }
                }
                
                with st.spinner("Creating job..."):
                    response = client.make_request('jobs', method='POST', data=job_data)
                    if response:
                        st.success("Job created successfully!")
                        st.json(response)

def show_dashboard():
    col1, col2 = st.columns(2)
    client = OpenEOClient()
    
    with col1:
        st.subheader("Active Jobs")
        jobs = client.make_request('jobs')
        if jobs:
            active_jobs = [job for job in jobs.get('jobs', []) if job.get('status') != 'finished']
            df = pd.DataFrame(active_jobs)
            if not df.empty:
                st.dataframe(df)
            else:
                st.info("No active jobs")
    
    with col2:
        st.subheader("Available Collections")
        collections = client.make_request('collections')
        if collections:
            df = pd.DataFrame(collections.get('collections', []))
            if not df.empty:
                st.dataframe(df)

def show_collections():
    st.header("Collections")
    client = OpenEOClient()
    collections = client.make_request('collections')
    
    if collections:
        for collection in collections.get('collections', []):
            with st.expander(f"{collection.get('id', 'Unknown Collection')}"):
                st.json(collection)
                if st.button(f"Show Details", key=f"details_{collection.get('id')}"):
                    details = client.make_request(f"collections/{collection.get('id')}")
                    if details:
                        st.json(details)

def show_jobs():
    st.header("Jobs")
    client = OpenEOClient()
    visualizer = DataVisualizer(client)
    
    if 'refresh_counter' not in st.session_state:
        st.session_state.refresh_counter = 0
    if 'delete_confirm' not in st.session_state:
        st.session_state.delete_confirm = False
    if 'selected_job' not in st.session_state:
        st.session_state.selected_job = None
        
    auto_refresh = st.sidebar.checkbox("Auto-refresh")
    if auto_refresh:
        st.session_state.refresh_counter += 1
        st.rerun()
    
    if st.button("Refresh Jobs"):
        st.session_state.refresh_counter += 1
        st.rerun()
    
    with st.spinner("Loading jobs..."):
        jobs = client.make_request('jobs')
    
    if jobs:
        df = pd.DataFrame(jobs.get('jobs', []))
        if not df.empty:
            def style_status(val):
                colors = {
                    'created': 'blue',
                    'running': 'orange',
                    'finished': 'green',
                    'error': 'red'
                }
                return f"color: {colors.get(val, 'black')}"
                
            styled_df = df.style.applymap(style_status, subset=['status'])
            st.dataframe(styled_df)
            
            selected_job = st.selectbox(
                    "Select Job",
                    df['id'].tolist()
                    )
            st.session_state.selected_job = selected_job
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("Start Job"):
                    with st.spinner("Starting job..."):
                        response = client.make_request(
                            f"jobs/{selected_job}/results", 
                            method='POST'
                        )
                        if response:
                            st.success(f"Job {selected_job} started!")
                            st.json(response)
            
            with col2:
                if st.button("Get Results"):
                    with st.spinner("Fetching results..."):
                        job_status = client.make_request(f"jobs/{selected_job}")
                        if job_status:
                            status = job_status.get('status')
                            if status != 'finished':
                                st.warning(f"Job is not finished yet. Current status: {status}")
                            else:
                                try:
                                    results = client.make_request(f"jobs/{selected_job}/results")
                                    if results:
                                        st.success("Results retrieved successfully")
                                        st.json(results)
                                except Exception as e:
                                    st.error(f"Error retrieving results: {str(e)}")
            
            with col3:
                if not st.session_state.delete_confirm:
                    if st.button("Delete Job"):
                        st.session_state.delete_confirm = True
                        st.rerun()
                else:
                    st.warning(f"Delete job {st.session_state.selected_job}?")
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Confirm"):
                            response = client.make_request(
                                f"jobs/{st.session_state.selected_job}", 
                                method='DELETE'
                            )
                            if response is not None:
                                st.session_state.delete_confirm = False
                                time.sleep(0.5)
                                st.rerun()
                            st.success("Job deleted!")
                    with col2:
                        if st.button("Cancel"):
                            st.session_state.delete_confirm = False
                            st.rerun()

        # Visualization section
            with col4:
                viz_type = st.selectbox(
                    "Visualization Type",
                    ["Spatial", "Time Series"],
                    key="viz_type"
                )
                
                if st.button("Visualize"):
                    job_status = client.make_request(f"jobs/{selected_job}")
                    
                    # Debug output
                    st.write("Debug - Job Status:", job_status)
                    
                    if job_status and job_status.get('status') == 'finished':
                        with st.spinner("Creating visualization..."):
                            try:
                                # Hole zuerst die Job-Ergebnisse
                                results = client.make_request(f"jobs/{selected_job}/results")
                                st.write("Debug - Job Results:", results)
                                
                                if results:
                                    data_url = results.get('assets', {}).get('data', {}).get('href')
                                    if not data_url:
                                        st.error("No data URL found in results")
                                        return
                                        
                                    st.write("Debug - Data URL:", data_url)
                                    
                                    try:
                                        if viz_type == "Spatial":
                                            with st.spinner("Creating spatial visualization..."):
                                                fig = visualizer.visualize_spatial(selected_job)
                                                if fig:
                                                    st.pyplot(fig)
                                                    plt.close(fig)  # Clean up
                                                else:
                                                    st.error("No visualization data returned")
                                        else:  # Time Series
                                            with st.spinner("Creating time series visualization..."):
                                                fig = visualizer.visualize_time_series(selected_job)
                                                if fig:
                                                    st.pyplot(fig)
                                                    plt.close(fig)  # Clean up
                                                else:
                                                    st.error("No visualization data returned")
                                    except Exception as viz_error:
                                        st.error(f"Visualization error: {str(viz_error)}")
                                        import traceback
                                        st.code(traceback.format_exc())
                                else:
                                    st.error("Could not fetch job results")
                            except Exception as e:
                                st.error(f"Error accessing results: {str(e)}")
                                import traceback
                                st.code(traceback.format_exc())
                    else:
                        st.warning("Job must be finished before visualization")

        else:
            st.info("No jobs available")
    else:
        st.error("Could not fetch jobs")

def main():
    st.set_page_config(page_title="GUI for OpenEO API on Rasdaman DB", layout="wide")
    st.title("GUI for OpenEO API on Rasdaman DB")
    
    page = st.sidebar.selectbox(
        "Navigation",
        ["Dashboard", "Collections", "Jobs", "Create Job"]
    )
    
    if page == "Dashboard":
        show_dashboard()
    elif page == "Collections":
        show_collections()
    elif page == "Jobs":
        show_jobs()
    elif page == "Create Job":
        create_job_section()

if __name__ == '__main__':
    main()