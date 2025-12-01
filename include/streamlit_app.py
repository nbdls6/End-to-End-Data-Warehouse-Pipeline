import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page configuration
st.set_page_config(
    page_title="NBA Player Statistics Dashboard",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("🏀 NBA Player Statistics Dashboard")
st.markdown("**Data Source:** Snowflake | **Schema:** MARMOT_FINAL")

# Snowflake connection function
@st.cache_resource
def get_snowflake_connection():
    """Create and cache Snowflake connection"""
    try:
        # Hardcoded Snowflake credentials (using private key like dbt)
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        
        # Load private key - use relative path to script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        private_key_path = os.path.join(script_dir, 'rsa_key.pem')
        
        with open(private_key_path, 'rb') as key:
            p_key = serialization.load_pem_private_key(
                key.read(),
                password=None,  # Empty passphrase
                backend=default_backend()
            )
        
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user='MARMOT',
            account='azb79167',
            warehouse='TRAINING_WH',
            database='MLDS430',
            schema='MARMOT_FINAL',
            role='TRAINING_ROLE',
            private_key=pkb
        )
        return conn
    except Exception as e:
        st.error(f"❌ Connection error: {str(e)}")
        st.info("💡 Make sure Snowflake credentials and private key are configured correctly")
        return None

# Initialize connection
conn = get_snowflake_connection()

if conn is None:
    st.stop()

# Sidebar filters
st.sidebar.header("🔍 Filters")

# Get available seasons
@st.cache_data
def get_seasons():
    """Get list of available seasons"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT season_year 
            FROM fact_player_season 
            ORDER BY season_year DESC
        """)
        seasons = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return seasons
    except Exception as e:
        st.error(f"Error fetching seasons: {str(e)}")
        return []

# Get available players
@st.cache_data
def get_players():
    """Get list of available players"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT player_name 
            FROM fact_player_season 
            ORDER BY player_name
        """)
        players = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return players
    except Exception as e:
        st.error(f"Error fetching players: {str(e)}")
        return []

seasons = get_seasons()
players = get_players()

# Sidebar filters
selected_season = st.sidebar.selectbox(
    "Select Season",
    options=["All"] + seasons,
    index=0
)

# Combined searchable player selector
# Streamlit selectbox supports typing to filter, so we can use it directly
# But to make it more user-friendly, we'll show all players and let them type to search
selected_player = st.sidebar.selectbox(
    "Select Player (optional)",
    options=["All"] + sorted(players),  # Show all players, sorted alphabetically
    index=0,
    help="Type to search for a player (e.g., LeBron, Kobe, Curry...)"
)

# Build SQL query
def build_query():
    """Build SQL query based on filters"""
    base_query = """
        SELECT 
            f.season_year,
            f.player_name,
            f.position,
            f.team_code,
            f.games_played,
            f.minutes_played,
            f.points_per_game,
            f.rebounds_per_game,
            f.assists_per_game,
            f.steals_per_game,
            f.blocks_per_game,
            a.PER as player_efficiency_rating,
            a.TS_PCT as true_shooting_pct,
            a.WS as win_shares,
            a.BPM as box_plus_minus,
            a.VORP as value_over_replacement_player
        FROM fact_player_season f
        LEFT JOIN fact_player_season_advanced a
            ON f.season_year = a.season_year 
            AND f.player_name = a.player_name
        WHERE 1=1
    """
    
    if selected_season != "All":
        base_query += f" AND f.season_year = {selected_season}"
    
    if selected_player != "All":
        base_query += f" AND f.player_name = '{selected_player}'"
    
    base_query += " ORDER BY f.season_year DESC, f.points_per_game DESC"
    
    return base_query

# Main content area
tab1, tab2, tab3 = st.tabs(["📊 Overview", "📈 Player Performance", "🏆 Top Performers"])

with tab1:
    st.header("Season Overview")
    
    try:
        cursor = conn.cursor()
        query = build_query()
        cursor.execute(query)
        
        # Fetch data
        columns = [desc[0].lower() for desc in cursor.description]  # Convert to lowercase
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)
        cursor.close()
        
        if len(df) > 0:
            # Check if required columns exist
            required_columns = ['player_name', 'season_year', 'points_per_game', 'player_efficiency_rating']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                st.error(f"Missing columns: {missing_columns}")
                st.info(f"Available columns: {list(df.columns)}")
                with st.expander("View Raw Data"):
                    st.dataframe(df.head())
                st.stop()
            
            # Key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Players", len(df['player_name'].unique()))
            with col2:
                st.metric("Total Seasons", len(df['season_year'].unique()))
            with col3:
                st.metric("Avg Points/Game", f"{df['points_per_game'].mean():.1f}")
            with col4:
                # Handle NULL values in PER
                per_mean = df['player_efficiency_rating'].dropna().mean()
                st.metric("Avg PER", f"{per_mean:.1f}" if not pd.isna(per_mean) else "N/A")
            
            st.divider()
            
            # Visualization 1: Points per Game Over Time
            st.subheader("📈 Points Per Game Trends by Season")
            
            # Aggregate by season
            season_stats = df.groupby('season_year').agg({
                'points_per_game': 'mean',
                'player_name': 'count'
            }).reset_index()
            season_stats.columns = ['season_year', 'avg_points_per_game', 'player_count']
            season_stats.columns = ['season_year', 'avg_points_per_game', 'player_count']
            
            fig1 = px.line(
                season_stats,
                x='season_year',
                y='avg_points_per_game',
                title='Average Points Per Game by Season',
                labels={'season_year': 'Season Year', 'avg_points_per_game': 'Average Points Per Game'},
                markers=True
            )
            fig1.update_layout(
                xaxis_title="Season Year",
                yaxis_title="Average Points Per Game",
                hovermode='x unified'
            )
            st.plotly_chart(fig1, use_container_width=True)
            
            # Visualization 2: Top 10 Players by Points
            st.subheader("🏀 Top 10 Players by Points Per Game")
            
            top_players = df.nlargest(10, 'points_per_game')[['player_name', 'season_year', 'points_per_game', 'team_code']]
            
            fig2 = px.bar(
                top_players,
                x='points_per_game',
                y='player_name',
                orientation='h',
                color='season_year',
                title='Top 10 Players by Points Per Game',
                labels={'points_per_game': 'Points Per Game', 'player_name': 'Player'},
                color_continuous_scale='viridis'
            )
            fig2.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                height=400
            )
            st.plotly_chart(fig2, use_container_width=True)
            
            # Data table
            with st.expander("📋 View Full Data"):
                st.dataframe(df, use_container_width=True)
        else:
            st.warning("No data found for selected filters.")
            
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

with tab2:
    st.header("Player Performance Analysis")
    
    if selected_player != "All":
        try:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT 
                    f.season_year,
                    f.player_name,
                    f.position,
                    f.team_code,
                    f.games_played,
                    f.points_per_game,
                    f.rebounds_per_game,
                    f.assists_per_game,
                    f.steals_per_game,
                    f.blocks_per_game,
                    a.PER as player_efficiency_rating,
                    a.TS_PCT as true_shooting_pct,
                    a.WS as win_shares,
                    a.BPM as box_plus_minus
                FROM fact_player_season f
                LEFT JOIN fact_player_season_advanced a
                    ON f.season_year = a.season_year 
                    AND f.player_name = a.player_name
                WHERE f.player_name = '{selected_player}'
                ORDER BY f.season_year DESC
            """)
            
            columns = [desc[0].lower() for desc in cursor.description]  # Convert to lowercase
            data = cursor.fetchall()
            player_df = pd.DataFrame(data, columns=columns)
            cursor.close()
            
            if len(player_df) > 0:
                # Player career stats
                st.subheader(f"Career Stats for {selected_player}")
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Visualization 3: Player Career Performance
                    metrics = ['points_per_game', 'rebounds_per_game', 'assists_per_game']
                    fig3 = go.Figure()
                    
                    for metric in metrics:
                        fig3.add_trace(go.Scatter(
                            x=player_df['season_year'],
                            y=player_df[metric],
                            mode='lines+markers',
                            name=metric.replace('_', ' ').title(),
                            line=dict(width=2)
                        ))
                    
                    fig3.update_layout(
                        title=f'{selected_player} - Career Performance Trends',
                        xaxis_title='Season Year',
                        yaxis_title='Per Game Average',
                        hovermode='x unified',
                        height=400
                    )
                    st.plotly_chart(fig3, use_container_width=True)
                
                with col2:
                    st.metric("Career Avg PPG", f"{player_df['points_per_game'].mean():.1f}")
                    st.metric("Career Avg RPG", f"{player_df['rebounds_per_game'].mean():.1f}")
                    st.metric("Career Avg APG", f"{player_df['assists_per_game'].mean():.1f}")
                    st.metric("Career Avg PER", f"{player_df['player_efficiency_rating'].mean():.1f}")
                
                # Player data table
                st.dataframe(player_df, use_container_width=True)
            else:
                st.info(f"No data found for {selected_player}")
        except Exception as e:
            st.error(f"Error loading player data: {str(e)}")
    else:
        st.info("👈 Please select a player from the sidebar to view performance analysis")

with tab3:
    st.header("Top Performers")
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                f.player_name,
                f.season_year,
                f.points_per_game,
                f.rebounds_per_game,
                f.assists_per_game,
                a.PER as player_efficiency_rating,
                a.WS as win_shares,
                a.VORP as value_over_replacement_player
            FROM fact_player_season f
            LEFT JOIN fact_player_season_advanced a
                ON f.season_year = a.season_year 
                AND f.player_name = a.player_name
            ORDER BY value_over_replacement_player DESC
            LIMIT 20
        """)
        
        columns = [desc[0].lower() for desc in cursor.description]  # Convert to lowercase
        data = cursor.fetchall()
        top_df = pd.DataFrame(data, columns=columns)
        cursor.close()
        
        # Visualization 4: VORP Comparison
        st.subheader("🌟 Top 20 Players by VORP (Value Over Replacement Player)")
        
        fig4 = px.bar(
            top_df,
            x='value_over_replacement_player',
            y='player_name',
            orientation='h',
            color='player_efficiency_rating',
            title='Top Players by VORP',
            labels={
                'value_over_replacement_player': 'VORP',
                'player_name': 'Player',
                'player_efficiency_rating': 'PER'
            },
            color_continuous_scale='plasma'
        )
        fig4.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            height=600
        )
        st.plotly_chart(fig4, use_container_width=True)
        
        # Top performers table
        st.dataframe(top_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error loading top performers: {str(e)}")

# Footer
st.divider()
st.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
st.caption("Data Pipeline: Kaggle → Snowflake → dbt → Streamlit")

