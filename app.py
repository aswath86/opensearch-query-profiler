import streamlit as st
import requests
import json
import os
from requests.auth import HTTPBasicAuth
import plotly.graph_objects as go

st.set_page_config(page_title="OpenSearch Query Profiler", layout="wide")

def get_password():
    return os.getenv('OPENSEARCH_PASSWORD') or st.secrets.get("opensearch", {}).get("password")

def execute_query(endpoint, index, query, username, password):
    query_dict = json.loads(query)
    query_dict["profile"] = True
    
    endpoint = endpoint.rstrip('/')
    url = f"{endpoint}/{index}/_search?phase_took=true"
    
    try:
        response = requests.post(url, json=query_dict, auth=HTTPBasicAuth(username, password))
        if response.status_code != 200:
            st.error(f"HTTP {response.status_code}: {response.text}")
            return None
        return response.json()
    except Exception as e:
        st.error(f"Error: {e}")
        return None

def parse_profile(profile_data):
    shards = []
    for i, shard in enumerate(profile_data.get("shards", [])):
        shard_data = {
            "id": shard.get("id", f"shard_{i}"),
            "index": shard.get("id", "").split("[")[0] if "[" in shard.get("id", "") else "unknown",
            "searches": [],
            "aggregations": []
        }
        
        for search in shard.get("searches", []):
            search_data = {"queries": [], "collectors": []}
            
            for query in search.get("query", []):
                query_data = {
                    "type": query.get("type", "unknown"),
                    "description": query.get("description", ""),
                    "time_ms": query.get("time_in_nanos", 0) / 1_000_000,
                    "breakdown": [{"operation": op, "time_ms": time_ns / 1_000_000} 
                                 for op, time_ns in query.get("breakdown", {}).items()]
                }
                search_data["queries"].append(query_data)
            
            for collector in search.get("collector", []):
                collector_data = {
                    "name": collector.get("name", "unknown"),
                    "reason": collector.get("reason", ""),
                    "time_ms": collector.get("time_in_nanos", 0) / 1_000_000
                }
                search_data["collectors"].append(collector_data)
            
            shard_data["searches"].append(search_data)
        
        for agg in shard.get("aggregations", []):
            agg_data = {
                "type": agg.get("type", "unknown"),
                "description": agg.get("description", ""),
                "time_ms": agg.get("time_in_nanos", 0) / 1_000_000,
                "breakdown": [{"operation": op, "time_ms": time_ns / 1_000_000} 
                             for op, time_ns in agg.get("breakdown", {}).items()]
            }
            shard_data["aggregations"].append(agg_data)
        
        shards.append(shard_data)
    return shards

def create_shard_chart(shards):
    if not shards:
        return None
    
    shard_data = []
    for s in shards:
        shard_time = sum(q['time_ms'] for search in s['searches'] for q in search['queries'])
        shard_data.append({'name': f"{s['index']}[{s['id']}]", 'time': shard_time})
    
    shard_data.sort(key=lambda x: x['time'], reverse=True)
    top_shards = shard_data[:10]
    
    shard_names = [s['name'] for s in reversed(top_shards)]
    shard_times = [s['time'] for s in reversed(top_shards)]
    
    fig = go.Figure(go.Bar(y=shard_names, x=shard_times, orientation='h', 
                          marker_color=shard_times, marker_colorscale='Viridis'))
    fig.update_layout(title="Slowest Shards", xaxis_title="Time (ms)", height=300)
    return fig

def create_breakdown_chart(breakdown_data, title):
    if not breakdown_data:
        return None
    
    ops = [item["operation"] for item in breakdown_data[:10]]
    times = [item["time_ms"] for item in breakdown_data[:10]]
    
    fig = go.Figure(go.Bar(y=ops, x=times, orientation='h', 
                          marker_color=times, marker_colorscale='Plasma'))
    fig.update_layout(title=title, xaxis_title="Time (ms)", height=400)
    return fig

def get_all_components(shards):
    components = []
    for shard in shards:
        shard_name = f"{shard['index']}[{shard['id']}]"
        
        for search in shard['searches']:
            for query in search['queries']:
                components.append({
                    "shard": shard_name,
                    "type": "Query",
                    "name": query['type'],
                    "time_ms": query['time_ms']
                })
            
            for collector in search['collectors']:
                components.append({
                    "shard": shard_name,
                    "type": "Collector",
                    "name": collector['name'],
                    "time_ms": collector['time_ms']
                })
        
        for agg in shard['aggregations']:
            components.append({
                "shard": shard_name,
                "type": "Aggregation",
                "name": agg['type'],
                "time_ms": agg['time_ms']
            })
    
    return sorted(components, key=lambda x: x['time_ms'], reverse=True)

def create_phase_chart(phase_took):
    if not phase_took:
        return None
    
    all_phases = ["dfs_pre_query", "query", "fetch", "dfs_query", "expand", "can_match"]
    phases = []
    times = []
    
    for phase in all_phases:
        phases.append(phase)
        times.append(phase_took.get(phase, 0))
    
    fig = go.Figure(go.Bar(y=phases, x=times, orientation='h',
                          marker_color=times, marker_colorscale='Blues'))
    fig.update_layout(title="Query Phases", xaxis_title="Time (ms)", yaxis_title="Phase", height=200)
    return fig

st.title("🔍 OpenSearch Query Profiler")

with st.sidebar:
    endpoint = st.text_input("Endpoint", "https://search-test-jsf53bqv7jph3j57yqtcjit3tq.us-east-1.es.amazonaws.com")
    index = st.text_input("Index", "opensearch_dashboards*")
    username = st.text_input("Username", "admin")
    password_input = st.text_input("Password", type="password")
    
    default_query = '''{
  "query": {
    "match_all": {}
  },
  "size": 10,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category.keyword",
        "size": 10
      }
    },
    "manufacturers": {
      "terms": {
        "field": "manufacturer.keyword",
        "size": 5
      }
    }
  }
}'''
    query = st.text_area("Query", default_query, height=250)
    
    execute_clicked = st.button("Execute", type="primary")
    
    st.divider()
    st.subheader("📄 Analyze Existing Profile")
    profile_response = st.text_area("Profile Response (JSON)", 
                                   placeholder="Paste your OpenSearch profile response here...", 
                                   height=200)
    
    analyze_clicked = st.button("Analyze Profile", type="secondary")
    
    # Handle Execute button
    if execute_clicked:
        if 'result' in st.session_state:
            del st.session_state.result
        
        password = password_input or get_password()
        if password:
            with st.spinner("Executing query..."):
                try:
                    result = execute_query(endpoint, index, query, username, password)
                    if result:
                        st.session_state.result = result
                        st.session_state.source = "execute"
                        st.success("Query executed successfully!")
                        st.rerun()
                    else:
                        st.error("Query execution failed")
                except Exception as e:
                    st.error(f"Execution error: {e}")
        else:
            st.error("Password not found")
    
    # Handle Analyze Profile button
    if analyze_clicked:
        if 'result' in st.session_state:
            del st.session_state.result
            
        if profile_response.strip():
            response_size = len(profile_response)
            if response_size > 10_000_000:
                st.error("Profile response too large (>10MB). Please use a smaller response.")
            else:
                with st.spinner(f"Analyzing profile response ({response_size:,} characters)..."):
                    try:
                        import re
                        cleaned_response = profile_response
                        
                        # Fix triple quotes in description fields
                        def fix_description(match):
                            content = match.group(1).replace('"', '\\"')
                            return '"description": "' + content + '"'
                        
                        cleaned_response = re.sub(
                            r'"description":\s*"""(.*?)"""',
                            fix_description,
                            cleaned_response,
                            flags=re.DOTALL
                        )
                        
                        result = json.loads(cleaned_response)
                        if "profile" in result:
                            profile_shards = len(result["profile"].get("shards", []))
                            if profile_shards > 100:
                                st.warning(f"Large profile with {profile_shards} shards. Processing may take time...")
                            
                            st.session_state.result = result
                            st.session_state.source = "analyze"
                            st.success(f"Profile loaded successfully! ({profile_shards} shards)")
                            st.rerun()
                        else:
                            st.error("No profile data found in response")
                    except json.JSONDecodeError as e:
                        st.error(f"JSON parsing failed: {str(e)[:200]}...")
                        st.info("The profile response contains invalid JSON. Try copying the response again.")
                    except Exception as e:
                        st.error(f"Analysis error: {str(e)[:200]}...")
        else:
            st.error("Please provide a profile response")

if hasattr(st.session_state, 'result'):
    result = st.session_state.result
    
    if "profile" not in result:
        st.error("No profile data in response")
        st.json(result)
        st.stop()
    
    shards = parse_profile(result["profile"])
    
    # Phase timing overview
    if "phase_took" in st.session_state.result:
        st.subheader("⏱️ Query & Fetch Phases")
        phase_took = st.session_state.result["phase_took"]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Query Phase", f"{phase_took.get('query', 0)}ms")
        with col2:
            st.metric("Fetch Phase", f"{phase_took.get('fetch', 0)}ms")
        with col3:
            st.metric("DFS Query", f"{phase_took.get('dfs_query', 0)}ms")
        with col4:
            st.metric("Can Match", f"{phase_took.get('can_match', 0)}ms")
        
        phase_fig = create_phase_chart(phase_took)
        if phase_fig:
            st.plotly_chart(phase_fig, use_container_width=True, config={'displayModeBar': True})
    
    # Shard level overview
    st.subheader("Shard Overview")
    shard_fig = create_shard_chart(shards)
    if shard_fig:
        st.plotly_chart(shard_fig, use_container_width=True, config={'displayModeBar': True})
    
    # Top slowest components
    all_components = get_all_components(shards)
    if all_components:
        st.subheader("🐌 Slowest Components")
        top_components = all_components[:10]
        
        comp_names = [f"{c['type']}: {c['name']}" for c in top_components]
        comp_times = [c['time_ms'] for c in top_components]
        
        fig = go.Figure(go.Bar(y=comp_names, x=comp_times, orientation='h', 
                              marker_color=comp_times, marker_colorscale='Reds'))
        fig.update_layout(title="Top 10 Slowest Components", xaxis_title="Time (ms)", height=400)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True})
    
    # All shards details
    if shards:
        st.subheader("Shard Details")
        for shard in shards:
            shard_name = f"{shard['index']}[{shard['id']}]"
            with st.expander(f"📊 {shard_name}", expanded=False):
                
                # Queries
                for search in shard['searches']:
                    if search['queries']:
                        st.write("**🔍 Queries:**")
                        for query in search['queries']:
                            st.write(f"• {query['type']}: {query['time_ms']:.2f}ms")
                            if query['breakdown']:
                                breakdown_fig = create_breakdown_chart(
                                    sorted(query['breakdown'], key=lambda x: x['time_ms'], reverse=True),
                                    f"Query: {query['type']}"
                                )
                                if breakdown_fig:
                                    st.plotly_chart(breakdown_fig, use_container_width=True, config={'displayModeBar': True})
                    
                    # Collectors
                    if search['collectors']:
                        st.write("**🗂️ Collectors:**")
                        for collector in search['collectors']:
                            st.write(f"• {collector['name']}: {collector['time_ms']:.2f}ms")
                            if collector['reason']:
                                st.write(f"  Reason: {collector['reason']}")
                
                # Aggregations
                if shard['aggregations']:
                    st.write("**📈 Aggregations:**")
                    for agg in shard['aggregations']:
                        st.write(f"• {agg['type']}: {agg['time_ms']:.2f}ms")
                        if agg['breakdown']:
                            breakdown_fig = create_breakdown_chart(
                                sorted(agg['breakdown'], key=lambda x: x['time_ms'], reverse=True),
                                f"Aggregation: {agg['type']}"
                            )
                            if breakdown_fig:
                                st.plotly_chart(breakdown_fig, use_container_width=True, config={'displayModeBar': True})