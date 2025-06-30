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
                    "time_ms": collector.get("time_in_nanos", 0) / 1_000_000,
                    "children": []
                }
                
                # Process child collectors
                for child in collector.get("children", []):
                    child_data = {
                        "name": child.get("name", "unknown"),
                        "reason": child.get("reason", ""),
                        "time_ms": child.get("time_in_nanos", 0) / 1_000_000
                    }
                    collector_data["children"].append(child_data)
                
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

# Show overall query time if available
if hasattr(st.session_state, 'result') and 'took' in st.session_state.result:
    st.metric("Overall Query Time", f"{st.session_state.result['took']}ms")

with st.sidebar:
    st.image("https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg", width=150)
    endpoint = st.text_input("Endpoint", "http://localhost:9200")
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
                            content = match.group(1)
                            # Escape quotes but preserve the full content
                            content = content.replace('"', '\\"')
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
    



def build_operation_tree(query_data, parent_time=None):
    """Build hierarchical operation tree"""
    operations = []
    
    if isinstance(query_data, list):
        for item in query_data:
            operations.extend(build_operation_tree(item, parent_time))
    elif isinstance(query_data, dict):
        time_ns = query_data.get('time_in_nanos', 0)
        time_ms = time_ns / 1_000_000
        
        operation = {
            'type': query_data.get('type', 'Unknown'),
            'description': query_data.get('description', ''),
            'time_ms': time_ms,
            'time_ns': time_ns,
            'breakdown': query_data.get('breakdown', {}),
            'children': []
        }
        
        # Calculate percentage of parent time
        if parent_time and parent_time > 0:
            operation['percentage'] = (time_ms / parent_time) * 100
        else:
            operation['percentage'] = 100
            
        # Process children recursively
        if 'children' in query_data and query_data['children']:
            operation['children'] = build_operation_tree(query_data['children'], time_ms)
        
        operations.append(operation)
    
    return operations

def calculate_self_time(operation):
    """Calculate self time (total time - children time)"""
    children_time = sum(child['time_ms'] for child in operation.get('children', []))
    return operation['time_ms'] - children_time

def get_color_for_percentage(percentage):
    """Get color based on time percentage"""
    if percentage >= 80:
        return "#ff6b6b"  # Red
    elif percentage >= 60:
        return "#ffa726"  # Orange  
    elif percentage >= 40:
        return "#ffeb3b"  # Yellow
    elif percentage >= 20:
        return "#66bb6a"  # Light Green
    else:
        return "#e0e0e0"  # Gray

def create_breakdown_chart_from_list(breakdown_list, title):
    """Create breakdown chart from list format"""
    if not breakdown_list or len(breakdown_list) <= 1:
        return None
    
    # Filter out zero time operations and sort by time
    filtered_breakdown = [item for item in breakdown_list if item.get('time_ms', 0) > 0]
    filtered_breakdown.sort(key=lambda x: x.get('time_ms', 0), reverse=True)
    
    if len(filtered_breakdown) <= 1:
        return None
    
    ops = [item.get('operation', '').replace('_', ' ').title() for item in filtered_breakdown[:8]]
    times = [item.get('time_ms', 0) for item in filtered_breakdown[:8]]
    
    fig = go.Figure(go.Bar(
        y=ops[::-1], 
        x=times[::-1], 
        orientation='h',
        marker_color=times[::-1], 
        marker_colorscale='Viridis'
    ))
    fig.update_layout(
        title=title, 
        xaxis_title="Time (ms)", 
        height=max(200, len(ops) * 30),
        showlegend=False
    )
    return fig

def create_breakdown_chart_from_dict(breakdown_dict, title):
    """Create breakdown chart from dict format"""
    if not breakdown_dict:
        return None
    
    breakdown_data = []
    for key, value in breakdown_dict.items():
        if not key.endswith('_count') and isinstance(value, (int, float)) and value > 0:
            time_ms = value / 1_000_000 if value > 1000 else value
            breakdown_data.append({
                'operation': key,
                'time_ms': time_ms
            })
    
    return create_breakdown_chart_from_list(breakdown_data, title)

def create_collector_chart(collector_data, title):
    """Create chart for collector data"""
    if not collector_data or len(collector_data) <= 1:
        return None
    
    # Sort by time
    sorted_collectors = sorted(collector_data, key=lambda x: x.get('time_ms', 0), reverse=True)
    
    names = [item['name'] for item in sorted_collectors[:10]]
    times = [item['time_ms'] for item in sorted_collectors[:10]]
    
    fig = go.Figure(go.Bar(
        y=names[::-1], 
        x=times[::-1], 
        orientation='h',
        marker_color=times[::-1], 
        marker_colorscale='Oranges'
    ))
    fig.update_layout(
        title=title, 
        xaxis_title="Time (ms)", 
        height=max(200, len(names) * 30),
        showlegend=False
    )
    return fig

def display_operation_tree(operations, level=0, total_time=None, unique_prefix=""):
    """Display operations in a tree structure"""
    for i, op in enumerate(operations):
        # Calculate percentage relative to total query time
        if total_time and total_time > 0:
            total_percentage = (op['time_ms'] / total_time) * 100
        else:
            total_percentage = op['percentage']
            
        self_time = calculate_self_time(op)
        color = get_color_for_percentage(total_percentage)
        
        # Create indentation for tree structure
        indent_spaces = "　" * level
        tree_symbol = "├─ " if level > 0 else "🔍 "
        
        # Operation header container
        with st.container():
            # Operation header with timing info
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                # Operation name with tree structure
                st.markdown(f"**{indent_spaces}{tree_symbol}{op['type']}**")
                if op['description'] and level < 3:  # Only show description for top levels
                    with st.container():
                        st.code(op['description'], language='text')
            
            with col2:
                st.metric("Self Time", f"{self_time:.2f}ms")
            
            with col3:
                st.metric("Total Time", f"{op['time_ms']:.2f}ms")
                
            with col4:
                # Color-coded percentage badge
                st.markdown(f"""
                <div style="
                    background-color: {color}; 
                    color: white; 
                    padding: 4px 8px; 
                    border-radius: 4px; 
                    text-align: center; 
                    font-weight: bold;
                    font-size: 12px;
                ">
                    {total_percentage:.1f}%
                </div>
                """, unsafe_allow_html=True)
            
            # Show breakdown if available and at top levels
            if op['breakdown'] and isinstance(op['breakdown'], dict) and level < 2:
                breakdown_data = []
                total_breakdown_time = sum(v for k, v in op['breakdown'].items() if not k.endswith('_count') and isinstance(v, (int, float)))
                
                for key, value in op['breakdown'].items():
                    if not key.endswith('_count') and isinstance(value, (int, float)) and value > 0:
                        time_ms = value / 1_000_000
                        relative_pct = (value / total_breakdown_time * 100) if total_breakdown_time > 0 else 0
                        breakdown_data.append({
                            'Operation': key.replace('_', ' ').title(),
                            'Time (ms)': f"{time_ms:.3f}",
                            'Relative %': f"{relative_pct:.1f}%"
                        })
                
                if breakdown_data and len(breakdown_data) > 1:
                    breakdown_data.sort(key=lambda x: float(x['Time (ms)']), reverse=True)
                    
                    # Create breakdown chart
                    ops = [item['Operation'] for item in breakdown_data[:8]]
                    times = [float(item['Time (ms)']) for item in breakdown_data[:8]]
                    
                    if len(times) > 1:
                        fig = go.Figure(go.Bar(
                            y=ops[::-1], 
                            x=times[::-1], 
                            orientation='h',
                            marker_color=times[::-1], 
                            marker_colorscale='Viridis'
                        ))
                        fig.update_layout(
                            title=f"Query Breakdown: {op['type']}", 
                            xaxis_title="Time (ms)", 
                            height=max(200, len(ops) * 30),
                            showlegend=False
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Show breakdown table with button toggle (with unique prefix)
                    button_key = f"query_breakdown_{unique_prefix}_{level}_{i}"
                    if button_key not in st.session_state:
                        st.session_state[button_key] = False
                    
                    col_btn, col_info = st.columns([3, 1])
                    with col_btn:
                        if st.button(f"📋 {'Hide' if st.session_state[button_key] else 'Show'} Query Breakdown Table ({len(breakdown_data)} operations)", key=button_key + "_btn"):
                            st.session_state[button_key] = not st.session_state[button_key]
                    
                    if st.session_state[button_key]:
                        st.dataframe(breakdown_data, use_container_width=True, hide_index=True)
            
            # Add visual separator for tree levels
            if level < 3:
                st.markdown("---")
        
        # Recursively display children (limit depth to avoid too much nesting)
        if op['children'] and level < 3:
            display_operation_tree(op['children'], level + 1, total_time, unique_prefix)

# Query Profile Tree Section 
if hasattr(st.session_state, 'result') and shards:
    st.subheader("🌳 Query Profile Tree")
    st.write("*Hierarchical view of query execution*")
    
    # Group shards by index
    index_groups = {}
    for shard in shards:
        index_name = shard['index']
        if index_name not in index_groups:
            index_groups[index_name] = []
        index_groups[index_name].append(shard)
    
    # Display each index
    for index_idx, (index_name, index_shards) in enumerate(index_groups.items()):
        st.write(f"### 📂 Index: **{index_name}**")
        
        # Calculate total index time
        total_index_time = sum(
            sum(q['time_ms'] for search in shard['searches'] for q in search['queries'])
            for shard in index_shards
        )
        
        if total_index_time > 0:
            st.write(f"**Cumulative time:** {total_index_time:.2f}ms")
        
        # Display each shard in the index (make each shard collapsible and collapsed by default)
        for shard_idx, shard in enumerate(sorted(index_shards, key=lambda s: sum(q['time_ms'] for search in s['searches'] for q in search['queries']), reverse=True)):
            shard_time = sum(q['time_ms'] for search in shard['searches'] for q in search['queries'])
            shard_percentage = (shard_time / total_index_time * 100) if total_index_time > 0 else 0
            shard_color = get_color_for_percentage(shard_percentage)
            
            # Make each shard collapsible (collapsed by default)
            with st.expander(
                f"🔍 Shard [{shard['id']}] - {shard_time:.2f}ms ({shard_percentage:.1f}%)", 
                expanded=False
            ):
                # Shard time badge
                st.markdown(f"""
                <div style="
                    background-color: {shard_color}; 
                    color: white; 
                    padding: 8px 16px; 
                    border-radius: 8px; 
                    text-align: center; 
                    font-weight: bold;
                    margin-bottom: 16px;
                ">
                    Shard Time: {shard_time:.2f}ms ({shard_percentage:.1f}%)
                </div>
                """, unsafe_allow_html=True)
                
                # Process searches for this shard
                for search_idx, search in enumerate(shard['searches']):
                    if search['queries']:
                        # Build operation tree from raw search data
                        if hasattr(st.session_state, 'result'):
                            original_shards = st.session_state.result.get('profile', {}).get('shards', [])
                            
                            # Find the matching original shard data
                            original_shard = None
                            for orig_shard in original_shards:
                                if shard['id'] in orig_shard.get('id', ''):
                                    original_shard = orig_shard
                                    break
                            
                            if original_shard and 'searches' in original_shard:
                                for orig_search_idx, orig_search in enumerate(original_shard['searches']):
                                    if orig_search_idx == search_idx and 'query' in orig_search:
                                        # Calculate total search time for percentage calculations
                                        total_search_time = sum(q['time_ms'] for q in search['queries'])
                                        
                                        # Build and display operation tree with unique prefix
                                        operations = build_operation_tree(orig_search['query'])
                                        
                                        if operations:
                                            st.write("**Query Operations:**")
                                            # Create unique prefix for this shard/search combination
                                            unique_prefix = f"{index_idx}_{shard_idx}_{search_idx}_{orig_search_idx}"
                                            display_operation_tree(operations, total_time=total_search_time, unique_prefix=unique_prefix)
                                        
                                        # Show rewrite time if available
                                        if 'rewrite_time' in orig_search and orig_search['rewrite_time'] > 0:
                                            rewrite_ms = orig_search['rewrite_time'] / 1_000_000
                                            st.info(f"🔄 **Rewrite Time:** {rewrite_ms:.3f}ms")
                
                # Show collectors information (with charts and button-toggle tables)
                if any(search.get('collectors') for search in shard['searches']):
                    st.write("**Collectors:**")
                    all_collectors = []
                    for search in shard['searches']:
                        for collector in search.get('collectors', []):
                            all_collectors.append({
                                'name': collector['name'],
                                'time_ms': collector['time_ms'],
                                'reason': collector.get('reason', ''),
                                'children': collector.get('children', [])
                            })
                            # Add children as separate entries
                            for child in collector.get('children', []):
                                all_collectors.append({
                                    'name': f"  └─ {child['name']}",
                                    'time_ms': child['time_ms'],
                                    'reason': child.get('reason', ''),
                                    'children': []
                                })
                    
                    # Create collector chart
                    if len(all_collectors) > 1:
                        collector_fig = create_collector_chart(all_collectors, "Collector Performance")
                        if collector_fig:
                            st.plotly_chart(collector_fig, use_container_width=True)
                    
                    # Show collector details in containers
                    for collector_idx, search in enumerate(shard['searches']):
                        for coll_idx, collector in enumerate(search.get('collectors', [])):
                            with st.container():
                                st.write(f"📋 **{collector['name']}** ({collector['time_ms']:.2f}ms)")
                                if collector.get('reason'):
                                    st.write(f"   Reason: {collector['reason']}")
                                
                                if collector.get('children'):
                                    # Prepare collector table data
                                    child_data = []
                                    for child in collector['children']:
                                        child_data.append({
                                            'Name': child['name'],
                                            'Reason': child.get('reason', ''),
                                            'Time (ms)': f"{child['time_ms']:.3f}"
                                        })
                                    
                                    if child_data:
                                        # Button toggle for collector children (with unique key)
                                        collector_button_key = f"collector_{index_idx}_{shard_idx}_{collector_idx}_{coll_idx}"
                                        if collector_button_key not in st.session_state:
                                            st.session_state[collector_button_key] = False
                                        
                                        if st.button(f"📋 {'Hide' if st.session_state[collector_button_key] else 'Show'} Collector Details ({len(child_data)} child collectors)", key=collector_button_key + "_btn"):
                                            st.session_state[collector_button_key] = not st.session_state[collector_button_key]
                                        
                                        if st.session_state[collector_button_key]:
                                            st.dataframe(child_data, use_container_width=True, hide_index=True)
                                
                                st.markdown("---")
                
                # Show aggregations if any (with charts and button-toggle tables)
                if shard['aggregations']:
                    st.write("**Aggregations:**")
                    for agg_idx, agg in enumerate(shard['aggregations']):
                        with st.container():
                            st.write(f"📊 **{agg['type']}** ({agg['time_ms']:.2f}ms)")
                            if agg['description']:
                                st.code(agg['description'])
                            
                            # Handle breakdown with charts
                            if agg['breakdown']:
                                if isinstance(agg['breakdown'], list):
                                    # Create chart for list format
                                    breakdown_fig = create_breakdown_chart_from_list(
                                        agg['breakdown'], 
                                        f"Aggregation Breakdown: {agg['type']}"
                                    )
                                    if breakdown_fig:
                                        st.plotly_chart(breakdown_fig, use_container_width=True)
                                    
                                    # Show breakdown table with button toggle (with unique key)
                                    breakdown_data = []
                                    for breakdown_item in agg['breakdown']:
                                        if isinstance(breakdown_item, dict) and breakdown_item.get('time_ms', 0) > 0:
                                            breakdown_data.append({
                                                'Operation': breakdown_item.get('operation', '').replace('_', ' ').title(),
                                                'Time (ms)': f"{breakdown_item['time_ms']:.3f}"
                                            })
                                    
                                    if breakdown_data:
                                        breakdown_data.sort(key=lambda x: float(x['Time (ms)']), reverse=True)
                                        # Button toggle for aggregation breakdown
                                        agg_button_key = f"agg_list_{index_idx}_{shard_idx}_{agg_idx}"
                                        if agg_button_key not in st.session_state:
                                            st.session_state[agg_button_key] = False
                                        
                                        if st.button(f"📋 {'Hide' if st.session_state[agg_button_key] else 'Show'} Aggregation Breakdown Table ({len(breakdown_data)} operations)", key=agg_button_key + "_btn"):
                                            st.session_state[agg_button_key] = not st.session_state[agg_button_key]
                                        
                                        if st.session_state[agg_button_key]:
                                            st.dataframe(breakdown_data, use_container_width=True, hide_index=True)
                                        
                                elif isinstance(agg['breakdown'], dict):
                                    # Create chart for dict format
                                    breakdown_fig = create_breakdown_chart_from_dict(
                                        agg['breakdown'], 
                                        f"Aggregation Breakdown: {agg['type']}"
                                    )
                                    if breakdown_fig:
                                        st.plotly_chart(breakdown_fig, use_container_width=True)
                                    
                                    # Show breakdown table with button toggle (with unique key)
                                    breakdown_data = []
                                    for key, value in agg['breakdown'].items():
                                        if not key.endswith('_count') and isinstance(value, (int, float)) and value > 0:
                                            time_ms = value / 1_000_000 if value > 1000 else value
                                            breakdown_data.append({
                                                'Operation': key.replace('_', ' ').title(),
                                                'Time (ms)': f"{time_ms:.3f}"
                                            })
                                    
                                    if breakdown_data:
                                        breakdown_data.sort(key=lambda x: float(x['Time (ms)']), reverse=True)
                                        # Button toggle for aggregation breakdown
                                        agg_button_key = f"agg_dict_{index_idx}_{shard_idx}_{agg_idx}"
                                        if agg_button_key not in st.session_state:
                                            st.session_state[agg_button_key] = False
                                        
                                        if st.button(f"📋 {'Hide' if st.session_state[agg_button_key] else 'Show'} Aggregation Breakdown Table ({len(breakdown_data)} operations)", key=agg_button_key + "_btn"):
                                            st.session_state[agg_button_key] = not st.session_state[agg_button_key]
                                        
                                        if st.session_state[agg_button_key]:
                                            st.dataframe(breakdown_data, use_container_width=True, hide_index=True)
                            
                            st.markdown("---")