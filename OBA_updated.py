# Optimized connection pool and login handling
import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import pooling
import hmac
import pytz
import threading
import schedule
import time
from datetime import datetime
from contextlib import contextmanager
import requests
import functools
from typing import Dict, List, Tuple, Optional, Any, Union

# ============ CONFIGURATION ============
st.set_page_config(
    page_title="NYC Procurement Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
PAGE_SIZE = 50
TARGET_TZ = pytz.timezone('America/New_York')
CONNECTION_POOL_LOCK = threading.Lock()
CONNECTION_POOL = None
SCRAPER_LOCK = threading.Lock()

# Optimized pool config (reduced size for faster initial connection)
# Optimized pool config for faster initial connection
POOL_CONFIG = {
    "pool_name": "mypool",
    "pool_size": 5,  # Reduced further from 3 to 2
    "pool_reset_session": False,  # Changed from True to avoid unnecessary overhead
    "autocommit": True,
    "use_pure": False,  # Changed from True for better performance
    "connection_timeout": 3,  # Reduced timeout from 5 to 3 seconds
    "consume_results": True , # Add this to automatically consume results
    "ssl_disabled": True  # add this to db_config

}

# ============ DATABASE OPERATIONS ============

# 2. Implement lazy connection initialization
@st.cache_resource(show_spinner="Connecting to database...", ttl=360000)
def get_connection_pool():
    """Singleton pattern with lazy initialization for connection pool"""
    global CONNECTION_POOL
    
    if CONNECTION_POOL is not None:
        return CONNECTION_POOL
    
    with CONNECTION_POOL_LOCK:
        if CONNECTION_POOL is None:
            try:
                # Add connection validation
                db_config = {
                    "host": st.secrets.mysql.host,
                    "user": st.secrets.mysql.user,
                    "password": st.secrets.mysql.password,
                    "database": st.secrets.mysql.database,
                    "port": st.secrets.mysql.port,
                    "connect_timeout": 3,  # Shorter timeout
                    
                }
                pool_settings = {**POOL_CONFIG, **db_config}
                CONNECTION_POOL = mysql.connector.pooling.MySQLConnectionPool(**pool_settings)
            except Exception as e:
                print(f"❌ Failed to create connection pool: {e}")
                # Try fallback to direct connection
                try:
                    CONNECTION_POOL = SimpleFallbackPool(db_config)
                except:
                    raise
    return CONNECTION_POOL

# Simple fallback if pooling fails
class SimpleFallbackPool:
    def __init__(self, config):
        self.config = config
    
    def get_connection(self):
        return mysql.connector.connect(**self.config)


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = get_connection_pool().get_connection()
        yield conn
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"⚠️ Error closing connection: {e}")

@contextmanager
def get_db_cursor(dictionary=False):
    """Context manager that handles both connection and cursor"""
    with get_db_connection() as conn:
        if conn is None:
            yield None
        else:
            cursor = conn.cursor(dictionary=dictionary)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise
            finally:
                cursor.close()

def execute_query(query: str, params=None, fetch_all=True, as_dict=False) -> Union[List[Dict], Dict, None]:
    """Optimized query execution with error handling"""
    try:
        with get_db_cursor(dictionary=as_dict) as cursor:
            if cursor:
                cursor.execute(query, params or [])
                return cursor.fetchall() if fetch_all else cursor.fetchone()
            return [] if fetch_all else None
    except Exception as e:
        st.error(f"⚠️ Database error: {e}")
        return [] if fetch_all else None

# Create and optimize database indexes
# Optimized index creation with batching and transaction
def create_indexes():
    """Create database indexes for performance optimization in background"""
    def create_all_indexes():
        time.sleep(10)  # Wait longer after app initialization
        
        # All indexes
        all_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_services ON newtable (`Services Descrption`(255))",
            "ALTER TABLE newtable ADD FULLTEXT INDEX IF NOT EXISTS ft_services (`Services Descrption`)",
            "CREATE INDEX IF NOT EXISTS idx_agency ON newtable (Agency)",
            "CREATE INDEX IF NOT EXISTS idx_method ON newtable (`Procurement Method`)",
            "CREATE INDEX IF NOT EXISTS idx_fiscal_qtr ON newtable (`Fiscal Quarter`)",
            "CREATE INDEX IF NOT EXISTS idx_job_titles ON newtable (`Job Titles`)",
            "CREATE INDEX IF NOT EXISTS idx_agency_method ON newtable (Agency, `Procurement Method`)",
            "CREATE INDEX IF NOT EXISTS idx_award_agency ON nycproawards4 (Agency)",
            "CREATE INDEX IF NOT EXISTS idx_award_title ON nycproawards4 (Title(255))",
            "CREATE INDEX IF NOT EXISTS idx_award_date ON nycproawards4 (`Award Date`)",
            "CREATE INDEX IF NOT EXISTS idx_award_category ON nycproawards4 (Category)",
            "ALTER TABLE nycproawards4 ADD FULLTEXT INDEX IF NOT EXISTS ft_award_title (Title)",
            "ALTER TABLE nycproawards4 ADD FULLTEXT INDEX IF NOT EXISTS ft_award_description (Description)"
        ]
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for query in all_indexes:
                    try:
                        cursor.execute(query)
                        conn.commit()  # Commit after each index
                    except mysql.connector.Error as err:
                        if err.errno not in (1061, 1831):  # Ignore if index already exists
                            print(f"Error creating index: {err}")
    
    # Start background thread for all indexes
    threading.Thread(target=create_all_indexes, daemon=True).start()

# ============ DATA OPERATIONS ============

# Improved caching strategy with proper TTL
# Cache unique values with longer TTL and pre-computed common values
@st.cache_data(ttl=86400)  # Cache for 24 hours instead of 1 hour
def get_unique_values(column: str) -> List[str]:
    """Get unique values for a column with optimized query and caching"""
    # Pre-compute common values to avoid database queries

    
    # Optimize query with LIMIT clause to prevent full table scan
    query = f"""
        SELECT DISTINCT `{column}` 
        FROM newtable 
        WHERE `{column}` IS NOT NULL AND `{column}` != '' 
        ORDER BY `{column}`
        LIMIT 500
    """
    
   
    result = execute_query(query)
    return [row[0] for row in result] if result else []
    

@st.cache_data(ttl=36000, show_spinner="Searching...")
def search_data_all(keyword: str, agency: str, procurement_method: str, 
                   fiscal_quarter: str, job_titles: str) -> pd.DataFrame:
    """
    Search the main contracts table with optimized indexing
    """
    # Start with a basic query
    query_parts = ["SELECT * FROM newtable"]
    where_clauses = []
    params = []
    
    # Build optimized query based on provided filters
    # Order matters for index usage - put the most selective conditions first
    
    if keyword:
        # FULLTEXT is generally faster for text searches
        try:
            where_clauses.append("MATCH(`Services Descrption`) AGAINST (%s IN BOOLEAN MODE)")
            params.append(f"{keyword}*")
        except mysql.connector.Error:
            where_clauses.append("`Services Descrption` LIKE %s")
            params.append(f"%{keyword}%")
    
    # Add other filters - order by selectivity
     # First handle exact matches (best for indexes)
    filter_conditions = []
    
    if agency:
        filter_conditions.append(("Agency = %s", agency))
    if procurement_method:
        filter_conditions.append(("`Procurement Method` = %s", procurement_method))
    if fiscal_quarter:
        filter_conditions.append(("`Fiscal Quarter` = %s", fiscal_quarter))
    if job_titles:
        filter_conditions.append(("`Job Titles` = %s", job_titles))
    
    # Sort filters by selectivity (put most selective first)
    # This helps MySQL choose the right index
    for condition, value in filter_conditions:
        where_clauses.append(condition)
        params.append(value)
    
    # Handle fulltext search last (or first if it's very selective)
    if keyword:
        # Use MATCH AGAINST for better performance with fulltext index
        try:
            # Only use FULLTEXT if we have a proper term (not just wildcards)
            clean_keyword = keyword.strip()
            if len(clean_keyword) >= 3:  # MySQL requires min 3 chars for FULLTEXT
                where_clauses.append("MATCH(`Services Descrption`) AGAINST (%s IN BOOLEAN MODE)")
                params.append(f"{clean_keyword}*")
            else:
                # Fall back to LIKE for very short terms
                where_clauses.append("`Services Descrption` LIKE %s")
                params.append(f"%{clean_keyword}%")
        except Exception:
            where_clauses.append("`Services Descrption` LIKE %s")
            params.append(f"%{keyword}%")
    
    if where_clauses:
        query_parts.append("WHERE " + " AND ".join(where_clauses))
    
    # Add LIMIT to prevent slow queries
    query_parts.append("LIMIT 1000")
    
    # Execute the query
    final_query = " ".join(query_parts)
    
    # For debugging: check if indexes are being used
    # check_index_usage(final_query, params)
    
    result = execute_query(final_query, params, as_dict=True)
    return pd.DataFrame(result) if result else pd.DataFrame()

@st.cache_data(ttl=600, show_spinner="Searching procurement awards...")
def search_proawards(keyword: str, page: int = 1, page_size: int = 50) -> Tuple[pd.DataFrame, int]:
    """Search procurement awards table with pagination and improved performance"""
    if not keyword:
        return pd.DataFrame(), 0
    
    try:
        # Try FULLTEXT search first (much faster)
        fulltext_base = "MATCH(Title, Description) AGAINST (%s IN BOOLEAN MODE)"
        count_query = f"SELECT COUNT(*) as count FROM nycproawards4 WHERE {fulltext_base}"
        
        with get_db_cursor(dictionary=True) as cursor:
            if cursor:
                cursor.execute(count_query, (f"{keyword}*",))
                result = cursor.fetchone()
                total_count = result['count'] if result else 0
        
        # Get paginated results
        query = f"""
            SELECT * FROM nycproawards4 
            WHERE {fulltext_base} 
            ORDER BY `Award Date` DESC
            LIMIT {page_size} OFFSET {(page-1)*page_size}
        """
        
        with get_db_cursor(dictionary=True) as cursor:
            if cursor:
                cursor.execute(query, (f"{keyword}*",))
                result = cursor.fetchall()
                
        if result:
            return pd.DataFrame(result), total_count
            
    except mysql.connector.Error:
        # Fall back to LIKE (slower but more reliable)
        base_query = "Title LIKE %s OR Description LIKE %s"
        count_query = f"SELECT COUNT(*) as count FROM nycproawards4 WHERE {base_query}"
        params = [f"%{keyword}%", f"%{keyword}%"]
        
        with get_db_cursor(dictionary=True) as cursor:
            if cursor:
                cursor.execute(count_query, params)
                result = cursor.fetchone()
                total_count = result['count'] if result else 0
        
        query = f"""
            SELECT * FROM nycproawards4 
            WHERE {base_query} 
            ORDER BY `Award Date` DESC
            LIMIT {page_size} OFFSET {(page-1)*page_size}
        """
        
        result = execute_query(query, params, as_dict=True)
        return pd.DataFrame(result) if result else pd.DataFrame(), total_count
    
    return pd.DataFrame(), 0

# ============ BACKGROUND JOBS ============

def run_scraper():
    """Run the data scraper with improved error handling"""
    if not SCRAPER_LOCK.locked():
        with SCRAPER_LOCK:
            with st.spinner("Updating procurement data..."):
                try:
                    # Call the AWS Lambda function to trigger the scraper
                    url = "https://cn5o4mjltksnugq64yokmgt74q0psrik.lambda-url.us-east-2.on.aws/"
                    response = requests.get(url, timeout=10)
                    return True
                except requests.exceptions.RequestException as e:
                    st.error(f"Error calling scraper service: {e}")
                    return False

def run_scheduler():
    """Background thread for scheduled tasks"""
    while True:
        schedule.run_pending()
        time.sleep(1)

# ============ UI COMPONENTS ============

def pagination_ui(total_items: int, page_size: int = PAGE_SIZE, key: str = "pagination") -> int:
    """Create improved pagination controls and return the current page"""
    total_pages = max(1, (total_items + page_size - 1) // page_size)
    
    # Initialize page in session state if not exists
    if f"{key}_page" not in st.session_state:
        st.session_state[f"{key}_page"] = 1
    
    current_page = st.session_state[f"{key}_page"]
    
    # Create UI with better layout
    col1, col2, col3 = st.columns([1, 5, 1])
    
    with col1:
        if st.button("← Previous", key=f"{key}_prev", disabled=current_page <= 1):
            st.session_state[f"{key}_page"] -= 1
            st.rerun()
    
    with col2:
        # Use markdown with HTML to center the text
        st.markdown(f"<h6 style='text-align: center;'>Page {current_page} of {total_pages}</h6>", unsafe_allow_html=True)
    
    with col3:
        if st.button("Next →", key=f"{key}_next", disabled=current_page >= total_pages):
            st.session_state[f"{key}_page"] += 1
            st.rerun()
    
    return current_page

# Separate authentication from db initialization
def check_password() -> bool:
    """User authentication without database connection dependency"""
    def login_form():
        with st.form("login_form"):
            st.subheader("Login to NYC Procurement Intelligence")
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            submitted = st.form_submit_button("Log in", use_container_width=True)
            if submitted:
                password_entered()

    def password_entered():
        if st.session_state["username"] in st.secrets["passwords"] and hmac.compare_digest(
            st.session_state["password"],
            st.secrets.passwords[st.session_state["username"]],
        ):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if st.session_state.get("password_correct", False):
        return True

    login_form()
    if "password_correct" in st.session_state and not st.session_state["password_correct"]:
        st.error("😕 User not known or password incorrect")
    return False

def reset_all_states():
    """Reset all session state variables"""
    session_vars = [
        'search_clicked',
        'results',
        'selected_rows',
        'previous_selection',
        'editable_dataframe',
        'show_results',
        'show_awards',
        'show_matches',
        'keyword',
        'agency',
        'procurement_method',
        'fiscal_quarter',
        'job_titles',
        'primary_page',
        'awards_page'
    ]
    
    for var in session_vars:
        if var in st.session_state:
            del st.session_state[var]
    
    st.cache_data.clear()
    st.session_state.reset_trigger = True

def reset_search():
    """Reset only the keyword search"""
    st.session_state["keyword"] = ""

def format_dataframe_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format dataframe for better display"""
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Format currency columns if present
    currency_columns = ['Award Amount', 'Contract Amount']
    for col in currency_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) and str(x).replace('.', '').isdigit() else x)
    
    # Format date columns if present
    date_columns = ['Award Date',  'End Date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')
    
    return df



@st.cache_data(ttl=86400)  # Cache for 24 hours
def get_all_dropdown_values():
    """Pre-compute all dropdown values in a single function to avoid multiple DB calls"""
    return {
        "Agency": get_unique_values("Agency"),
        "Procurement Method": get_unique_values("Procurement Method"),
        "Fiscal Quarter": get_unique_values("Fiscal Quarter"),
        "Job Titles": get_unique_values("Job Titles")
    }




# ============ MAIN APPLICATION ============

def main():
    """Main application function with improved organization"""
    # Initialize session state variables
    if 'primary_page' not in st.session_state:
        st.session_state.primary_page = 1
    if 'awards_page' not in st.session_state:
        st.session_state.awards_page = 1
    if 'reset_trigger' not in st.session_state:
        st.session_state.reset_trigger = False
    if 'search_clicked' not in st.session_state:
        st.session_state.search_clicked = False
    if 'show_results' not in st.session_state:
        st.session_state.show_results = False
    if 'show_awards' not in st.session_state:
        st.session_state.show_awards = False
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()

    # Initialize database and background tasks
    if "indexes_created" not in st.session_state:
        with st.spinner("Optimizing database performance..."):
            create_indexes()
        st.session_state.indexes_created = True

    if "connection_tested" not in st.session_state:
        st.session_state["connection_tested"] = True
        get_connection_pool()

    if "scheduler_thread_started" not in st.session_state:
        st.session_state["scheduler_thread_started"] = True
        schedule.every().day.at("21:05").do(run_scraper)
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()

    for var in ['primary_page', 'awards_page', 'reset_trigger', 'search_clicked', 
                'show_results', 'show_awards', 'results']:
        if var not in st.session_state:
            st.session_state[var] = 1 if var.endswith('_page') else (
                pd.DataFrame() if var == 'results' else False)
    # Application header
    st.title("NYC Procurement Intelligence")
    st.markdown(
        "<h5 style='text-align: left; color: #636363;'>Pinpoint Commercial Opportunities with the City of New York</h5>",
        unsafe_allow_html=True,
    )
    
    # Current date display
    now = datetime.now(TARGET_TZ)
    formatted_date = now.strftime("%A, %B %d, %Y, %I:%M %p EDT")
    st.markdown(f"<p style='text-align:right; color: #636363;'>{formatted_date}</p>", unsafe_allow_html=True)
    if "initialization_started" not in st.session_state:
        st.session_state.initialization_started = True
        
        # Start a background thread for all initialization tasks
        def background_init():
            # Schedule the scraper
            schedule.every().day.at("21:05").do(run_scraper)
            scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
            scheduler_thread.start()
            
            # Create indexes in background
            create_indexes()
            
            # Mark indexes as created
            st.session_state.indexes_created = True
            
        # Start all initialization in background
        threading.Thread(target=background_init, daemon=True).start()

    

    # Sidebar setup
    st.sidebar.image("image001.png", use_container_width=True)
    st.sidebar.header("Search Filters")
    default_value = "" if st.session_state.get('reset_trigger', False) else st.session_state.get('keyword', "")
    default_index = 0 if st.session_state.get('reset_trigger', False) else None
    col1, col2 = st.sidebar.columns([8, 1])
    with col1:
            keyword = st.text_input(
                "Keyword Search (Services Description)",
                value=default_value,
                key="keyword"
            )
    with col2:
            st.write("") 
            st.button("X", on_click=reset_search, key="reset_keyword")
    # Search form in sidebar



    with st.sidebar.form(key="search_form"):
        default_value = "" if st.session_state.get('reset_trigger', False) else st.session_state.get('keyword', "")
        default_index = 0 if st.session_state.get('reset_trigger', False) else None
        
        
        dropdown_values = get_all_dropdown_values()

        agency = st.selectbox(
            "Agency",
            [""] + dropdown_values["Agency"],
            index=default_index,
            key="agency"
        )
        
        procurement_method = st.selectbox(
            "Procurement Method",
            [""] + dropdown_values["Procurement Method"],
            index=default_index,
            key="procurement_method"
        )
        
        fiscal_quarter = st.selectbox(
            "Fiscal Quarter",
            [""] + dropdown_values["Fiscal Quarter"],
            index=default_index,
            key="fiscal_quarter"
        )
        
        job_titles = st.selectbox(
            "Job Titles",
            [""] + dropdown_values["Job Titles"],
            index=default_index,
            key="job_titles"
        )

        # Search and reset buttons
        col1, col2 = st.columns(2)
        with col1:
            search_submitted = st.form_submit_button("Search", use_container_width=True)
        with col2:
            reset_submitted = st.form_submit_button("Reset", use_container_width=True)
        
        if reset_submitted:
            reset_all_states()
            st.rerun()

    # Separate action buttons
    if st.sidebar.button("Update Awards Data", use_container_width=True):
        success = run_scraper()
        if success:
            st.sidebar.success("Award data updated successfully!")
        else:
            st.sidebar.error("Failed to update award data. Please try again later.")

    # Reset session state reset_trigger if needed
    if st.session_state.get('reset_trigger', False):
        st.session_state.reset_trigger = False

    # Process search when submitted
    filters_applied = any([keyword, agency, procurement_method, fiscal_quarter, job_titles])
    
    if search_submitted:
        if filters_applied:
            st.session_state.search_clicked = True
            st.session_state.show_results = True
            st.session_state.show_awards = True
            
            # Reset to first page on new search
            st.session_state.primary_page = 1
            st.session_state.awards_page = 1
            
            # Search for primary contracts
            results_df = search_data_all(
                keyword, agency, procurement_method, fiscal_quarter, job_titles
            )
            st.session_state.results = results_df
            
            # Search for procurement awards if keyword provided
            if keyword:
                awards_df, awards_count = search_proawards(
                    keyword, 
                    page=1,
                    page_size=PAGE_SIZE
                )
                st.session_state.proawards_results = awards_df
                st.session_state.awards_count = awards_count
            else:
                st.session_state.proawards_results = pd.DataFrame()
                st.session_state.awards_count = 0
        else:
            st.warning("Please apply at least one filter before searching.")
            st.session_state.show_results = False
            st.session_state.show_awards = False

    # Display search results
    if st.session_state.search_clicked and filters_applied:
        # Handle no results case
        if st.session_state.results.empty and (not hasattr(st.session_state, 'proawards_results') or st.session_state.proawards_results.empty):
            st.warning("There are no results for this search. Please try different keywords or filters.")
        
        # Display primary contract matches
        if st.session_state.show_results and not st.session_state.results.empty:
            with st.expander("Primary Contract Matches", expanded=True):
                st.write(f"Found {len(st.session_state.results)} matching contracts")
                
                # Get paginated data for display
                total_primary_count = len(st.session_state.results)
                current_primary_page = st.session_state.primary_page
                start_idx = (current_primary_page - 1) * PAGE_SIZE
                end_idx = min(start_idx + PAGE_SIZE, total_primary_count)
                
                # Get subset of data for current page and format it
                current_page_data = st.session_state.results.iloc[start_idx:end_idx].copy()
                display_data = format_dataframe_for_display(current_page_data)
                
                # Display the formatted data
                st.dataframe(
                    display_data,
                    hide_index=True,
                    use_container_width=True,
                )
                
                # Show pagination controls
                new_page = pagination_ui(
                    total_primary_count, 
                    PAGE_SIZE, 
                    key="primary"
                )
                
                # Handle page change if needed
                if new_page != current_primary_page:
                    st.session_state.primary_page = new_page
                    st.rerun()
        
        # Display procurement awards
        if st.session_state.show_awards and hasattr(st.session_state, 'proawards_results') and not st.session_state.proawards_results.empty:
            with st.expander("Procurement Awards", expanded=True):
                awards_count = st.session_state.awards_count
                st.write(f"Found {awards_count} matching procurement awards")
                
                # Get current page of procurement awards
                current_awards_page = st.session_state.awards_page
                display_awards = format_dataframe_for_display(st.session_state.proawards_results)
                
                # Show awards data
                st.dataframe(
                    display_awards,
                    hide_index=True,
                    use_container_width=True,
                )
                
                # Show pagination for awards
                new_awards_page = pagination_ui(
                    awards_count,
                    PAGE_SIZE,
                    key="awards"
                )
                
                # Handle awards page change
                if new_awards_page != current_awards_page:
                    st.session_state.awards_page = new_awards_page
                    
                    # Update data for new page
                    awards_df, _ = search_proawards(
                        st.session_state.keyword,
                        page=new_awards_page,
                        page_size=PAGE_SIZE
                    )
                    st.session_state.proawards_results = awards_df
                    st.rerun()
        
        # Download button for combined data
        dfs_to_combine = []
        if not st.session_state.results.empty:
            dfs_to_combine.append(st.session_state.results)
        
        if hasattr(st.session_state, 'proawards_results') and not st.session_state.proawards_results.empty:
            dfs_to_combine.append(st.session_state.proawards_results)
        
        if dfs_to_combine:
            combined_df = pd.concat(dfs_to_combine, ignore_index=True)
            combined_df_filled = combined_df.fillna("N/A")
            csv = combined_df_filled.to_csv(index=False).encode('utf-8')
            
            # Add download section
            st.markdown("---")
            col1, col2 = st.columns([6, 1])
            with col1:
                st.write("Export all search results to CSV file:")
            with col2:
                st.download_button(
                    label="Download",
                    data=csv,
                    file_name=f'nyc_procurement_data_{datetime.now().strftime("%Y%m%d")}.csv',
                    mime='text/csv',
                    use_container_width=True
                )


if __name__ == "__main__":
    st.session_state.indexes_created = True
    if not check_password():
        st.stop()
    main()
