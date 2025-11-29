from neo4j import GraphDatabase
import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List

class KGVisualizer:
    def __init__(self, driver):
        self.driver = driver
    
    def get_graph_stats(self) -> Dict:
        """Get comprehensive KG stats"""
        with self.driver.session() as session:
            stats = session.run("""
                MATCH (n) 
                RETURN labels(n)[0] as label, count(n) as nodes
                ORDER BY nodes DESC
                
                UNION ALL
                
                MATCH ()-[r]->() 
                RETURN type(r) as label, count(r) as relationships
                ORDER BY relationships DESC
            """).data()
        return stats
    
    def get_sample_graph(self, limit: int = 50) -> Dict:
        """Get sample subgraph for visualization"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n)-[r]-(m)
                RETURN n, r, m, id(n) as n_id, id(m) as m_id, id(r) as r_id
                LIMIT $limit
            """, limit=limit)
            
            nodes = []
            edges = []
            node_ids = {}
            edge_id = 0
            
            for record in result:
                n = record['n']
                m = record['m']
                r = record['r']
                
                # Add nodes
                if id(n) not in node_ids:
                    node_ids[id(n)] = len(nodes)
                    nodes.append({
                        'id': len(nodes),
                        'label': n.labels[0] if n.labels else 'Node',
                        'name': n.get('name', n.get('id', str(n.id))),
                        'properties': dict(n)
                    })
                
                if id(m) not in node_ids:
                    node_ids[id(m)] = len(nodes)
                    nodes.append({
                        'id': len(nodes),
                        'label': m.labels[0] if m.labels else 'Node',
                        'name': m.get('name', m.get('id', str(m.id))),
                        'properties': dict(m)
                    })
                
                # Add edge
                edges.append({
                    'id': edge_id,
                    'source': node_ids[id(n)],
                    'target': node_ids[id(m)],
                    'type': r.type,
                    'properties': dict(r)
                })
                edge_id += 1
            
            return {'nodes': nodes, 'edges': edges}
    
    def create_plotly_graph(self, graph_data: Dict):
        """Create interactive Plotly graph"""
        nodes = graph_data['nodes']
        edges = graph_data['edges']
        
        # Node positions (simple force layout simulation)
        pos = {node['id']: (i % 10, i // 10) for i, node in enumerate(nodes)}
        
        # Edge traces
        edge_x, edge_y = [], []
        for edge in edges:
            x0, y0 = pos[edge['source']]
            x1, y1 = pos[edge['target']]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        edge_trace = go.Scatter(x=edge_x, y=edge_y,
                               line=dict(width=2, color='#888'),
                               hoverinfo='none',
                               mode='lines')
        
        # Node traces
        node_x, node_y, node_text, node_colors = [], [], [], []
        for node in nodes:
            x, y = pos[node['id']]
            node_x.append(x)
            node_y.append(y)
            node_text.append(f"{node['label']}: {node['name']}")
            node_colors.append(len(node['properties']) * 10)  # Color by property count
        
        node_trace = go.Scatter(x=node_x, y=node_y,
                               mode='markers+text',
                               hoverinfo='text',
                               text=node_text,
                               textposition="middle center",
                               marker=dict(size=20,
                                         color=node_colors,
                                         colorscale='Viridis',
                                         showscale=True,
                                         colorbar=dict(thickness=15, title="Properties")),
                               textfont=dict(size=10))
        
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title="🗺️ Knowledge Graph Visualization",
                           showlegend=False,
                           hovermode='closest',
                           margin=dict(b=20,l=5,r=5,t=40),
                           annotations=[ dict(
                               text="Nodes: {} | Edges: {}".format(len(nodes), len(edges)),
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.005, y=-0.002,
                               xanchor='left', yanchor='bottom',
                               font=dict(color="#888", size=10)
                           )],
                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                       ))
        
        return fig
