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
from flashtext import KeywordProcessor
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

POOL_CONFIG = {
    "pool_name": "mypool",
    "pool_size": 10,  
    "pool_reset_session": False, 
    "autocommit": True,
    "use_pure": False,  
    "connection_timeout": 3,  
    "consume_results": True ,
    "ssl_disabled": True  

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
        st.error(f"❌ Database query failed: {e}")
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
    query = "SELECT * FROM newtable WHERE 1=1"
    params = []
    
    if keyword:
        query += " AND `Services Descrption` LIKE %s"
        params.append(f"%{keyword}%")
    if agency:
        query += " AND Agency = %s"
        params.append(agency)
    if procurement_method:
        query += " AND `Procurement Method` = %s"
        params.append(procurement_method)
    if fiscal_quarter:
        query += " AND `Fiscal Quarter` = %s"
        params.append(fiscal_quarter)
    if job_titles:
        query += " AND `Job Titles` = %s"
        params.append(job_titles)
    
    result = execute_query(query, params, as_dict=True)
    return pd.DataFrame(result) if result else pd.DataFrame()

@st.cache_data(ttl=40600, show_spinner="Searching procurement awards...")
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
    
    # Create UI with columns taking entire width
    # Use custom CSS to make the columns take full width
    st.markdown("""
        <style>
        [data-testid="stHorizontalBlock"] {
            width: 100%;
            display: flex;
            justify-content: space-between;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Define columns with proper width proportions
    col1, col2, col3 = st.columns([1, 10, 1])
    
    with col1:
        if st.button("← Previous", key=f"{key}_prev", disabled=current_page <= 1, use_container_width=True):
            st.session_state[f"{key}_page"] -= 1
            st.rerun()
    
    with col2:
        # Use markdown with HTML to center the text
        st.markdown(f"<h6 style='text-align: center;'>Page {current_page} of {total_pages}</h6>", unsafe_allow_html=True)
    
    with col3:
        if st.button("Next →", key=f"{key}_next", disabled=current_page >= total_pages, use_container_width=True):
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
    # Test connection once using our new connection manager
    if "connection_tested" not in st.session_state:
        st.session_state["connection_tested"] = True
        # Initializing the connection pool will automatically verify the connection
        get_connection_pool()
    
    # Start scheduler thread only once
    if "scheduler_thread_started" not in st.session_state:
        st.session_state["scheduler_thread_started"] = True
        schedule.every().day.at("21:05").do(run_scraper)
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()

    if 'reset_trigger' not in st.session_state:
        st.session_state.reset_trigger = False

    if 'search_clicked' not in st.session_state:
        st.session_state.search_clicked = False
    if 'show_results' not in st.session_state:
        st.session_state.show_results = False
    if 'show_awards' not in st.session_state:
        st.session_state.show_awards = False
    if 'show_matches' not in st.session_state:
        st.session_state.show_matches = False
    if 'results' not in st.session_state:
        st.session_state.results = pd.DataFrame()
    if 'selected_rows' not in st.session_state:
        st.session_state.selected_rows = pd.DataFrame()
    if 'previous_selection' not in st.session_state:
        st.session_state.previous_selection = set()

    st.title("NYC Procurement Intelligence")
    st.markdown(
        "<h5 style='text-align: left; color: #888888;'>Pinpoint Commercial Opportunities with the City of New York</h5>",
        unsafe_allow_html=True,
    )
    st.sidebar.image("image001.png", use_container_width=True)
    st.sidebar.header("Search Filters")

    default_value = "" if st.session_state.get('reset_trigger', False) else st.session_state.get('keyword', "")
    default_index = 0 if st.session_state.get('reset_trigger', False) else None
    
    col1, col2 = st.sidebar.columns([6, 1])
    with col1:
            keyword = st.text_input(
                "Keyword Search (Services Description)",
                value=default_value,
                key="keyword"
            )
    with col2:
            st.markdown("<div style='margin-top: 27px;'></div>", unsafe_allow_html=True)
            st.button("X", on_click=reset_search, key="reset_keyword")
    
    agency = st.sidebar.selectbox(
        "Agency",
        [""] + get_unique_values("Agency"),
        index=default_index,
        key="agency"
    )
    
    procurement_method = st.sidebar.selectbox(
        "Procurement Method",
        [""] + get_unique_values("Procurement Method"),
        index=default_index,
        key="procurement_method"
    )
    
    fiscal_quarter = st.sidebar.selectbox(
        "Fiscal Quarter",
        [""] + get_unique_values("Fiscal Quarter"),
        index=default_index,
        key="fiscal_quarter"
    )
    
    job_titles = st.sidebar.selectbox(
        "Job Titles",
        [""] + get_unique_values("Job Titles"),
        index=default_index,
        key="job_titles"
    )
    
    

    if st.session_state.get('reset_trigger', False):
        st.session_state.reset_trigger = False

    filters_applied = any([keyword, agency, procurement_method, fiscal_quarter, job_titles])

    if st.sidebar.button("Search"):
        if filters_applied:
            st.session_state.search_clicked = True
            st.session_state.show_results = True
            st.session_state.show_awards = True
            st.session_state.show_matches = True
            st.session_state.results = search_data_all(
                keyword, agency, procurement_method, fiscal_quarter, job_titles
            )
        else:
            st.warning("Please apply at least one filter before searching.")
            st.session_state.show_results = False
            st.session_state.show_awards = False
            st.session_state.show_matches = False

    if st.sidebar.button("Reset Search"):
        reset_all_states()
        st.rerun()

    if st.sidebar.button("Update Awards Data"):
        with st.spinner("Processing..."):
            # Using the run_scraper function that properly manages connections
            run_scraper()
        st.success("Award update complete!")

    if st.session_state.show_results:
        if st.session_state.results.empty:
            st.warning("No result found")
        else:
            st.write(f"Found {len(st.session_state.results)} results:")
            total_results = len(st.session_state.results)

            # Get current page from session or default to 1
            current_page = st.session_state.get("results_page", 1)

            # Calculate start and end indices
            start_idx = (current_page - 1) * PAGE_SIZE
            end_idx = min(start_idx + PAGE_SIZE, total_results)

            # Paginate the data
            current_page_results = st.session_state.results.iloc[start_idx:end_idx]

            st.write(f"Showing results {start_idx + 1} to {end_idx} of {total_results}:")

            # Add checkbox column
            select_column = pd.DataFrame({'Select': False}, index=current_page_results.index)
            results_with_checkbox = pd.concat([select_column, current_page_results], axis=1)

            # Render editable data editor
            edited_df = st.data_editor(
                results_with_checkbox,
                hide_index=True,
                column_config={"Select": st.column_config.CheckboxColumn("Select", default=False)},
                disabled=results_with_checkbox.columns.drop('Select').tolist(),
                key="editable_dataframe",
                use_container_width=True,
            )

            # Track selections
            current_selection = set(edited_df[edited_df['Select']].index)
            new_selections = current_selection - st.session_state.previous_selection
            deselections = st.session_state.previous_selection - current_selection

            if not st.session_state.selected_rows.empty:
                new_rows = edited_df.loc[list(new_selections)].drop(columns=['Select'])
                st.session_state.selected_rows = pd.concat(
                    [st.session_state.selected_rows, new_rows], ignore_index=True
                )
                st.session_state.selected_rows = st.session_state.selected_rows[
                    ~st.session_state.selected_rows.index.isin(deselections)
                ]
            else:
                st.session_state.selected_rows = edited_df.loc[list(new_selections)].drop(columns=['Select'])

            st.session_state.previous_selection = current_selection
            
            # Render pagination UI and handle page change
            new_page = pagination_ui(total_results, PAGE_SIZE, key="results")
            if new_page != current_page:
                st.session_state.results_page = new_page
                st.rerun()
                
            if not st.session_state.selected_rows.empty:
                st.write("User Selected Records:")
                st.dataframe(st.session_state.selected_rows, hide_index=True)
        # Render pagination UI and handle page change
        

    if st.session_state.show_awards and filters_applied:
        st.markdown("Fiscal Year 2025 NYC Government Procurement Awards")
        
        # Build query using standard indexes
        where_clauses = []
        params = []
        
        # Use traditional LIKE query instead of FULLTEXT
        if keyword:
            where_clauses.append("(Title LIKE %s OR Description LIKE %s)")
            params.extend([f"%{keyword}%", f"%{keyword}%"])  # Add parameters for both columns
        
        if agency:
            # Use idx_award_agency index
            where_clauses.append("Agency = %s")
            params.append(agency)
            
        # Use the idx_award_date index for sorting
        order_clause = "ORDER BY `Award Date` DESC"
        
        # Build the query with WHERE clause if filters exist
        if where_clauses:
            query = f"SELECT * FROM nycproawards4 WHERE {' AND '.join(where_clauses)} {order_clause} "
        else:
            query = f"SELECT * FROM nycproawards4 {order_clause} "
        
        # Execute query
        awards_data = execute_query(query, params, as_dict=True)
        df_awards = pd.DataFrame(awards_data) if awards_data else pd.DataFrame()
        
        if df_awards.empty:
            st.warning("No result found")
        else:
            st.dataframe(df_awards, use_container_width=True)

            if st.session_state.show_matches and not st.session_state.selected_rows.empty and keyword:
                st.markdown("Keyword Matches")
                keyword_processor = KeywordProcessor()
                keyword_processor.add_keyword(keyword)

                matched_rows = []
                for _, row in st.session_state.selected_rows.iterrows():
                    if keyword_processor.extract_keywords(row['Services Descrption']):
                        matched_rows.append(row)

                for _, row in df_awards.iterrows():
                    if keyword_processor.extract_keywords(row['Title']):
                        matched_rows.append(row)

                if matched_rows:
                    st.dataframe(pd.DataFrame(matched_rows))
                else:
                    st.write("No keyword matches found.")
        
    if st.session_state.show_results and st.session_state.show_awards and 'df_awards' in locals():
        combined_df = pd.concat([st.session_state.results, df_awards], ignore_index=True)
        combined_df_filled = combined_df.fillna("N/A")

        csv = combined_df_filled.to_csv(index=False).encode('utf-8')

        st.download_button(
            label="Download Data Report",
            data=csv,
            file_name='combined_data.csv',
            mime='text/csv',
        )


if __name__ == "__main__":
    st.session_state.indexes_created = True
    if not check_password():
        st.stop()
    main()
