import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor
import numpy as np
from datetime import datetime, timedelta
import os

# Set page configuration
st.set_page_config(
    page_title="Fantasy Football Analytics",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection parameters
DB_PARAMS = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'database': os.environ.get('DB_NAME', 'fantasy_football'),
    'user': os.environ.get('DB_USER', 'fantasy'),
    'password': os.environ.get('DB_PASSWORD', 'fantasy123')
}

# Helper function to establish database connection
@st.cache_resource
def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    return psycopg2.connect(**DB_PARAMS)

# Helper function to execute query and return results as a DataFrame
@st.cache_data(ttl=3600)  # Cache for 1 hour
def execute_query(query, params=None):
    """Execute a query and return results as a DataFrame"""
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Sidebar filters
st.sidebar.title("Filter Options")

# Position filter
position_options = ['All'] + ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
selected_position = st.sidebar.selectbox("Position", position_options)

# Week filter
week_options = list(range(1, 18))  # NFL regular season weeks
selected_week = st.sidebar.selectbox("Week", week_options, index=0)

# Season filter
current_year = datetime.now().year
season_options = list(range(current_year-2, current_year+1))
selected_season = st.sidebar.selectbox("Season", season_options, index=len(season_options)-1)

# Scoring format filter
scoring_options = ['Standard', 'PPR', 'Half PPR']
selected_scoring = st.sidebar.selectbox("Scoring Format", scoring_options)

# Apply filters to query
position_filter = "" if selected_position == 'All' else f"AND p.position = '{selected_position}'"

# Main dashboard title
st.title("Fantasy Football Analytics Dashboard")

# Create tabs for different analysis views
tab1, tab2, tab3, tab4 = st.tabs(["Player Performance", "Team Analysis", "Projections", "Injuries"])

with tab1:
    st.header("Player Performance Analysis")
    
    # Top performers for selected week and season
    st.subheader(f"Top Performers - Week {selected_week}, {selected_season} Season")
    
    top_performers_query = f"""
    SELECT 
        p.name AS player_name,
        p.position,
        COALESCE(t.team_name, p.team) AS team,
        SUM(ps.fantasy_points) AS fantasy_points,
        STRING_AGG(ps.stat_type || ': ' || ps.value::text, ', ') AS key_stats
    FROM 
        player_stats ps
    JOIN 
        players p ON ps.player_id = p.player_id
    LEFT JOIN 
        teams t ON p.team = t.abbreviation
    WHERE 
        ps.week = {selected_week}
        AND ps.season = {selected_season}
        {position_filter}
    GROUP BY 
        p.name, p.position, COALESCE(t.team_name, p.team)
    ORDER BY 
        fantasy_points DESC
    LIMIT 10
    """
    
    top_performers_df = execute_query(top_performers_query)
    
    if not top_performers_df.empty:
        # Create a bar chart for top performers
        fig = px.bar(
            top_performers_df,
            x='player_name',
            y='fantasy_points',
            color='position',
            hover_data=['team', 'key_stats'],
            title=f"Top 10 Fantasy Performers - Week {selected_week}",
            labels={'player_name': 'Player', 'fantasy_points': 'Fantasy Points'},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Display the top performers data table
        st.dataframe(top_performers_df)
    else:
        st.info(f"No data available for Week {selected_week}, {selected_season} Season")
    
    # Player trend analysis
    st.subheader("Player Trend Analysis")
    
    # Player search box for trend analysis
    player_query = """
    SELECT player_id, name, position, team 
    FROM players 
    ORDER BY name
    """
    players_df = execute_query(player_query)
    
    if not players_df.empty:
        player_options = [f"{row['name']} ({row['position']} - {row['team']})" for _, row in players_df.iterrows()]
        selected_player_str = st.selectbox("Select Player", options=player_options)
        
        # Extract player_id from the selected player
        selected_player_name = selected_player_str.split(" (")[0]
        selected_player = players_df[players_df['name'] == selected_player_name].iloc[0]
        
        # Get player performance trend
        player_trend_query = f"""
        SELECT 
            ps.week,
            SUM(ps.fantasy_points) AS fantasy_points,
            STRING_AGG(ps.stat_type || ': ' || ps.value::text, ', ') AS stats
        FROM 
            player_stats ps
        WHERE 
            ps.player_id = {selected_player['player_id']}
            AND ps.season = {selected_season}
        GROUP BY 
            ps.week
        ORDER BY 
            ps.week
        """
        
        player_trend_df = execute_query(player_trend_query)
        
        if not player_trend_df.empty:
            # Create a line chart for player trend
            fig = px.line(
                player_trend_df,
                x='week',
                y='fantasy_points',
                markers=True,
                title=f"{selected_player['name']} Weekly Fantasy Points - {selected_season} Season",
                labels={'week': 'Week', 'fantasy_points': 'Fantasy Points'},
                height=400
            )
            
            # Add league average for comparison if available
            avg_query = f"""
            SELECT 
                ps.week,
                AVG(ps.fantasy_points) AS avg_fantasy_points
            FROM 
                player_stats ps
            JOIN 
                players p ON ps.player_id = p.player_id
            WHERE 
                p.position = '{selected_player['position']}'
                AND ps.season = {selected_season}
            GROUP BY 
                ps.week
            ORDER BY 
                ps.week
            """
            
            avg_df = execute_query(avg_query)
            
            if not avg_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=avg_df['week'],
                        y=avg_df['avg_fantasy_points'],
                        mode='lines',
                        name=f'Avg {selected_player["position"]}',
                        line=dict(dash='dash', color='gray')
                    )
                )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show detailed stats for the selected player
            st.write("Weekly Stats Breakdown:")
            st.dataframe(player_trend_df)
        else:
            st.info(f"No performance data available for {selected_player['name']} in the {selected_season} season.")
    else:
        st.warning("No player data available.")

with tab2:
    st.header("Team Analysis")
    
    # NFL team performance analysis
    st.subheader("NFL Team Offensive Performance")
    
    team_performance_query = f"""
    SELECT 
        COALESCE(t.team_name, p.team) AS team,
        AVG(ps.fantasy_points) AS avg_fantasy_points,
        COUNT(DISTINCT p.player_id) AS player_count
    FROM 
        player_stats ps
    JOIN 
        players p ON ps.player_id = p.player_id
    LEFT JOIN 
        teams t ON p.team = t.abbreviation
    WHERE 
        ps.season = {selected_season}
        AND p.position IN ('QB', 'RB', 'WR', 'TE')
    GROUP BY 
        COALESCE(t.team_name, p.team)
    ORDER BY 
        avg_fantasy_points DESC
    """
    
    team_performance_df = execute_query(team_performance_query)
    
    if not team_performance_df.empty:
        # Create a bar chart for team performance
        fig = px.bar(
            team_performance_df,
            x='team',
            y='avg_fantasy_points',
            color='avg_fantasy_points',
            color_continuous_scale='Viridis',
            title=f"Average Fantasy Points per Player by Team - {selected_season} Season",
            labels={'team': 'Team', 'avg_fantasy_points': 'Avg Fantasy Points'},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No team performance data available for {selected_season} Season")
    
    # Position distribution by team
    st.subheader("Fantasy Value by Position and Team")
    
    position_team_query = f"""
    SELECT 
        COALESCE(t.team_name, p.team) AS team,
        p.position,
        AVG(ps.fantasy_points) AS avg_fantasy_points,
        COUNT(DISTINCT p.player_id) AS player_count
    FROM 
        player_stats ps
    JOIN 
        players p ON ps.player_id = p.player_id
    LEFT JOIN 
        teams t ON p.team = t.abbreviation
    WHERE 
        ps.season = {selected_season}
        AND p.position IN ('QB', 'RB', 'WR', 'TE')
    GROUP BY 
        COALESCE(t.team_name, p.team), p.position
    ORDER BY 
        team, p.position
    """
    
    position_team_df = execute_query(position_team_query)
    
    if not position_team_df.empty:
        # Create a heatmap for position distribution
        fig = px.density_heatmap(
            position_team_df,
            x='position',
            y='team',
            z='avg_fantasy_points',
            color_continuous_scale='Viridis',
            title=f"Average Fantasy Points by Position and Team - {selected_season} Season",
            labels={'position': 'Position', 'team': 'Team', 'avg_fantasy_points': 'Avg Fantasy Points'},
            height=700
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No position distribution data available for {selected_season} Season")

with tab3:
    st.header("Player Projections")
    
    # Filter for upcoming week projections
    upcoming_week = min(selected_week + 1, 17)
    
    st.subheader(f"Week {upcoming_week} Projections - {selected_season} Season")
    
    projections_query = f"""
    SELECT 
        p.name AS player_name,
        p.position,
        COALESCE(t.team_name, p.team) AS team,
        pj.source,
        pj.projected_points,
        pj.stat_category,
        pj.projected_value
    FROM 
        projections pj
    JOIN 
        players p ON pj.player_id = p.player_id
    LEFT JOIN 
        teams t ON p.team = t.abbreviation
    WHERE 
        pj.week = {upcoming_week}
        AND pj.season = {selected_season}
        {position_filter}
    ORDER BY 
        pj.projected_points DESC, p.name
    """
    
    projections_df = execute_query(projections_query)
    
    if not projections_df.empty:
        # Create a pivot table to compare projections from different sources
        pivot_df = projections_df.pivot_table(
            index=['player_name', 'position', 'team'],
            columns='source',
            values='projected_points',
            aggfunc='mean'
        ).reset_index()
        
        # Calculate average projection across sources
        sources = [col for col in pivot_df.columns if col not in ['player_name', 'position', 'team']]
        pivot_df['avg_projection'] = pivot_df[sources].mean(axis=1)
        
        # Create a grouped bar chart for projections comparison
        top_players = pivot_df.sort_values('avg_projection', ascending=False).head(15)
        
        fig = go.Figure()
        
        for source in sources:
            fig.add_trace(go.Bar(
                x=top_players['player_name'],
                y=top_players[source],
                name=source.capitalize()
            ))
        
        fig.add_trace(go.Scatter(
            x=top_players['player_name'],
            y=top_players['avg_projection'],
            mode='markers',
            marker=dict(color='black', size=10),
            name='Average'
        ))
        
        fig.update_layout(
            title=f"Top 15 Players - Week {upcoming_week} Projections",
            xaxis_title="Player",
            yaxis_title="Projected Fantasy Points",
            barmode='group',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add filters for specific positions
        if selected_position == 'All':
            position_tabs = st.tabs(['QB', 'RB', 'WR', 'TE'])
            
            for i, pos in enumerate(['QB', 'RB', 'WR', 'TE']):
                with position_tabs[i]:
                    pos_df = pivot_df[pivot_df['position'] == pos].sort_values('avg_projection', ascending=False).head(10)
                    
                    if not pos_df.empty:
                        st.write(f"Top 10 {pos} Projections:")
                        st.dataframe(pos_df)
                    else:
                        st.info(f"No projection data available for {pos} position")
        else:
            st.write(f"Top {selected_position} Projections:")
            pos_df = pivot_df[pivot_df['position'] == selected_position].sort_values('avg_projection', ascending=False)
            st.dataframe(pos_df)
    else:
        st.info(f"No projection data available for Week {upcoming_week}, {selected_season} Season")

with tab4:
    st.header("Injury Report")
    
    # Current injuries
    st.subheader("Current Player Injuries")
    
    injuries_query = f"""
    SELECT 
        p.name AS player_name,
        p.position,
        COALESCE(t.team_name, p.team) AS team,
        i.injury_type,
        i.body_part,
        i.practice_status,
        i.game_status,
        i.report_date,
        i.notes
    FROM 
        injuries i
    JOIN 
        players p ON i.player_id = p.player_id
    LEFT JOIN 
        teams t ON p.team = t.abbreviation
    WHERE 
        i.return_date IS NULL
        {position_filter}
    ORDER BY 
        i.report_date DESC, p.name
    """
    
    injuries_df = execute_query(injuries_query)
    
    if not injuries_df.empty:
        # Create injury status visualization
        status_counts = injuries_df['game_status'].value_counts().reset_index()
        status_counts.columns = ['game_status', 'count']
        
        # Define status order
        status_order = ['Out', 'Doubtful', 'Questionable', 'Probable']
        
        # Filter and sort status_counts
        status_counts = status_counts[status_counts['game_status'].isin(status_order)]
        status_counts['game_status'] = pd.Categorical(
            status_counts['game_status'], 
            categories=status_order, 
            ordered=True
        )
        status_counts = status_counts.sort_values('game_status')
        
        fig = px.bar(
            status_counts,
            x='game_status',
            y='count',
            color='game_status',
            color_discrete_map={
                'Out': 'red',
                'Doubtful': 'orange',
                'Questionable': 'yellow',
                'Probable': 'green'
            },
            title="Current Injuries by Game Status",
            labels={'game_status': 'Game Status', 'count': 'Number of Players'},
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Show injury details table
        st.write("Injury Details:")
        st.dataframe(injuries_df, hide_index=True)
        
        # Injury impact analysis
        st.subheader("Injury Impact Analysis")
        
        # Get injury impact by position
        position_impact = injuries_df['position'].value_counts().reset_index()
        position_impact.columns = ['position', 'count']
        
        fig = px.pie(
            position_impact,
            values='count',
            names='position',
            title="Injuries by Position",
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Get injury impact by team
        team_impact = injuries_df['team'].value_counts().reset_index().head(10)
        team_impact.columns = ['team', 'count']
        
        fig = px.bar(
            team_impact,
            x='team',
            y='count',
            title="Top 10 Teams Affected by Injuries",
            labels={'team': 'Team', 'count': 'Number of Injured Players'},
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No current injury data available")

# Footer with refresh information
st.sidebar.markdown("---")
st.sidebar.write(f"Data last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
if st.sidebar.button("Refresh Data"):
    st.experimental_rerun()