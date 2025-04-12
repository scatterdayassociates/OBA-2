import streamlit as st
import pandas as pd
import mysql.connector
from mysql.connector import pooling
import hmac
from flashtext import KeywordProcessor
import pytz
import threading
import schedule
import time
from datetime import datetime
from contextlib import contextmanager

st.set_page_config(layout="wide")
target_tz = pytz.timezone('America/New_York')
CONNECTION_POOL_LOCK = threading.Lock()
CONNECTION_POOL = None

POOL_CONFIG = {
    "pool_name": "mypool",
    "pool_size": 5, 
    "pool_reset_session": True,
    "autocommit": True, 
    "use_pure": True, 
}

def get_connection_pool():
    """Singleton pattern with lazy initialization for connection pool"""
    global CONNECTION_POOL
    
    if CONNECTION_POOL is not None:
        return CONNECTION_POOL
    
    with CONNECTION_POOL_LOCK:
        if CONNECTION_POOL is None:
            try:
                db_config = {
                    "host": st.secrets.mysql.host,
                    "user": st.secrets.mysql.user,
                    "password": st.secrets.mysql.password,
                    "database": st.secrets.mysql.database,
                    "port": st.secrets.mysql.port,
                }
                pool_settings = {**POOL_CONFIG, **db_config}
                CONNECTION_POOL = mysql.connector.pooling.MySQLConnectionPool(**pool_settings)
                print("✅ Connection pool created successfully")
            except Exception as e:
                print(f"❌ Failed to create connection pool: {e}")
                raise
    return CONNECTION_POOL

@contextmanager
def get_db_connection():
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
            with conn.cursor(dictionary=dictionary) as cursor:  
                try:
                    yield cursor
                    conn.commit()  
                except Exception as e:
                    conn.rollback() 
                    raise

SCRAPER_LOCK = threading.Lock()
def run_scraper():
    if not SCRAPER_LOCK.locked(): 
        with SCRAPER_LOCK:
            print("Running scheduled scraper")
            try:
                from scrapper_mysql import scraper

                conn = get_connection_pool().get_connection()
                try:
                    scraper(conn)                    
                except Exception as e:
                    
                    st.error(f"Scraper error: {e}")
                finally:

                    if conn:
                        try:
                            conn.close()
                            print("Scraper connection closed")
                        except Exception as e:
                            print(f"Error closing scraper connection: {e}")
            except Exception as e:
                print(f"Error closing scraper connection: {e}")

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

def execute_query(query, params=None, fetch_all=True, as_dict=False):
    """Optimized query execution with error handling"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=as_dict) as cursor:
                cursor.execute(query, params or [])
                return cursor.fetchall() if fetch_all else cursor.fetchone()
    except Exception as e:
        print(f"⚠️ Database error: {e}")
        return [] if fetch_all else None

# Cache database results to reduce database load
@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_unique_values(column):
    query = f"SELECT DISTINCT `{column}` FROM newtable ORDER BY `{column}`"
    result = execute_query(query)
    return [row[0] for row in result] if result else []

@st.cache_data(ttl=3600,
show_spinner=False,
hash_funcs={
KeywordProcessor: lambda _: None, # Ignore keyword processor changes
mysql.connector.pooling.PooledMySQLConnection: id })  # Cache for 5 minutes
# Update your search function to get all data at once
def search_data_all(keyword, agency, procurement_method, fiscal_quarter, job_titles):
    """
    Search the main contracts table and return all matching records
    """
    query = "SELECT * FROM newtable WHERE 1=1"
    params = []
    
    # Use FULLTEXT search for keywords when available
    if keyword:
        try:
            # Try FULLTEXT search first
            fulltext_base = "MATCH(`Services Descrption`) AGAINST (%s IN BOOLEAN MODE)"
            query = f"SELECT * FROM newtable WHERE {fulltext_base}"
            
            with get_db_cursor(dictionary=True) as cursor:
                cursor.execute(query, (f"{keyword}*",))
                result = cursor.fetchall()
                
            if result:
                return pd.DataFrame(result)
                
        except mysql.connector.Error:
            # Fall back to LIKE if FULLTEXT is not available
            query = "SELECT * FROM newtable WHERE `Services Descrption` LIKE %s"
            params = [f"%{keyword}%"]
    
    # Apply other filters
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
    
    # Execute the query (without pagination)
    result = execute_query(query, params, as_dict=True)
    return pd.DataFrame(result) if result else pd.DataFrame()


# NEW: Search procurement awards table
@st.cache_data(ttl=300)  # Cache for 5 minutes
def search_proawards(keyword, page=1, page_size=50):
    """Search procurement awards table with pagination"""
    if not keyword:
        return pd.DataFrame(), 0
    
    try:
        # Try FULLTEXT search first
        fulltext_base = "MATCH(Title, Description) AGAINST (%s IN BOOLEAN MODE)"
        count_query = f"SELECT COUNT(*) FROM nycproawards4 WHERE {fulltext_base}"
        
        with get_db_cursor() as cursor:
            cursor.execute(count_query, (f"{keyword}*",))
            total_count = cursor.fetchone()[0]
        
        # Get paginated results
        query = f"SELECT * FROM nycproawards4 WHERE {fulltext_base} LIMIT {page_size} OFFSET {(page-1)*page_size}"
        
        with get_db_cursor(dictionary=True) as cursor:
            cursor.execute(query, (f"{keyword}*",))
            result = cursor.fetchall()
            
        if result:
            return pd.DataFrame(result) if result else pd.DataFrame(), total_count
            
    except mysql.connector.Error:
        # Fall back to LIKE
        base_query = "Title LIKE %s OR Description LIKE %s"
        count_query = f"SELECT COUNT(*) FROM nycproawards4 WHERE {base_query}"
        params = [f"%{keyword}%", f"%{keyword}%"]
        
        with get_db_cursor() as cursor:
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
        
        query = f"SELECT * FROM nycproawards4 WHERE {base_query} LIMIT {page_size} OFFSET {(page-1)*page_size}"
        result = execute_query(query, params, as_dict=True)
        return pd.DataFrame(result) if result else pd.DataFrame(), total_count

def pagination_ui(total_items, page_size=50, key="pagination"):
    """Create pagination controls and return the current page"""
    total_pages = max(1, (total_items + page_size - 1) // page_size)
    print(total_pages)
    # Initialize page in session state if not exists
    if f"{key}_page" not in st.session_state:
        st.session_state[f"{key}_page"] = 1
    
    # Create UI
    col1, col2, col3 = st.columns([1,2,1])
    
    with col1:
        if st.button("← Previous", key=f"{key}_prev", disabled=st.session_state[f"{key}_page"] <= 1):
            st.session_state[f"{key}_page"] -= 1
            st.rerun()
    
    with col2:
        
        new_page = st.number_input(
            "Go to page", 
            min_value=1, 
            max_value=total_pages,
            value=st.session_state[f"{key}_page"],
            key=f"{key}_input"
        )
        if new_page != st.session_state[f"{key}_page"]:
            st.session_state[f"{key}_page"] = new_page
            st.rerun()
    
    with col3:
        if st.button("Next →", key=f"{key}_next", disabled=st.session_state[f"{key}_page"] >= total_pages):
            st.session_state[f"{key}_page"] += 1
            st.rerun()
    
    return st.session_state[f"{key}_page"]


def check_password():
    def login_form():
        with st.form("Credentials"):
            st.text_input("Username", key="username")
            st.text_input("Password", type="password", key="password")
            st.form_submit_button("Log in", on_click=password_entered)

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
    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")
    return False

def reset_all_states():
    session_vars = [
        'search_clicked',
        'results',
        'selected_rows',
        'previous_selection',
        'editable_dataframe',
        'show_results',
        'show_awards',
        'show_matches'
    ]
    
    for var in session_vars:
        if var in st.session_state:
            del st.session_state[var]
    
    st.cache_data.clear()
    
    st.session_state.reset_trigger = True
    st.rerun()

def reset_search():
    st.session_state["keyword"] = ""

def main():
    
    
    PAGE_SIZE = 50
    
    # Initialize pagination session state variables if not already present
    if 'primary_page' not in st.session_state:
        st.session_state.primary_page = 1
    if 'awards_page' not in st.session_state:
        st.session_state.awards_page = 1


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
    now = datetime.now(target_tz)
    formatted_date = now.strftime("%A, %B %d, %Y, %I:%M %p EDT")
    st.markdown(f"<p style='text-align:right'>{formatted_date}</p>", unsafe_allow_html=True)
    
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
            
            # Reset to first page on new search
            if "primary_page" in st.session_state:
                st.session_state.primary_page = 1
            if "awards_page" in st.session_state:
                st.session_state.awards_page = 1
            
            # Search with pagination
            results_df = search_data_all(
                keyword, agency, procurement_method, fiscal_quarter, job_titles,
                
                
            )
            st.session_state.results = results_df
          
            
            if keyword:
                awards_df, awards_count = search_proawards(
                    keyword, 
                    page=st.session_state.get("awards_page", 1),
                    page_size=50
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
            st.session_state.show_matches = False

    if st.sidebar.button("Reset Search"):
        reset_all_states()
        st.rerun()

    if st.sidebar.button("Update Awards Data"):
        with st.spinner("Processing..."):
   
            run_scraper()
        st.success("Award update complete!")
    if st.session_state.search_clicked and st.session_state.results.empty and filters_applied:
        st.warning("There are no results for this search. Please try different keywords or filters.")

    if st.session_state.show_results and not st.session_state.results.empty:
        st.subheader("Primary Contract Matches")
        
        # Get total count from session state
        total_primary_count = len(st.session_state.results)
       
        
        # Calculate start and end indices for the current page
        if 'primary_page' not in st.session_state:
            st.session_state.primary_page = 1
            
        current_primary_page = st.session_state.primary_page
        start_idx = (current_primary_page - 1) * PAGE_SIZE
        end_idx = min(start_idx + PAGE_SIZE, total_primary_count)
        
        # Get the subset of data for the current page
        current_page_data = st.session_state.results.iloc[start_idx:end_idx].copy()
        
        # Display the data editor first
        select_column = pd.DataFrame({'Select': False}, index=current_page_data.index)
        results_with_checkbox = pd.concat([select_column, current_page_data], axis=1)

        edited_df = st.data_editor(
            results_with_checkbox,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn("Select", default=False)},
            disabled=results_with_checkbox.columns.drop('Select').tolist(),
            key=f"editable_dataframe_{current_primary_page}",
            use_container_width=True,
        )
        
        # Show pagination controls AFTER the data editor
        new_page = pagination_ui(
            total_primary_count, 
            PAGE_SIZE, 
            key="primary"
        )
        
        # Handle page change if needed
        if new_page != current_primary_page:
            st.session_state.primary_page = new_page
            st.rerun()
        
        # Handle row selection
        if 'previous_selection' not in st.session_state:
            st.session_state.previous_selection = set()
        if 'selected_rows' not in st.session_state:
            st.session_state.selected_rows = pd.DataFrame()
            
        current_selection = set(edited_df[edited_df['Select']].index)
        new_selections = current_selection - st.session_state.previous_selection
        deselections = st.session_state.previous_selection - current_selection

        if not st.session_state.selected_rows.empty:
            new_rows = edited_df.loc[list(new_selections)].drop(columns=['Select']) if new_selections else pd.DataFrame()
            st.session_state.selected_rows = pd.concat(
                [st.session_state.selected_rows, new_rows], ignore_index=True
            ) if not new_rows.empty else st.session_state.selected_rows
            
            if deselections:
                # Handle deselections - may need adjustment based on your data structure
                deselected_indices = list(deselections)
                st.session_state.selected_rows = st.session_state.selected_rows[
                    ~st.session_state.selected_rows.index.isin(deselected_indices)
                ]
        else:
            st.session_state.selected_rows = edited_df.loc[list(new_selections)].drop(columns=['Select']) if new_selections else pd.DataFrame()
            
        st.session_state.previous_selection = current_selection

        if not st.session_state.selected_rows.empty:
            st.write("User Selected Records:")
            st.dataframe(st.session_state.selected_rows, hide_index=True)


            #sgrgerhr
            
    if st.session_state.show_awards and st.session_state.search_clicked and keyword:
        st.subheader("Fiscal Year 2025 NYC Government Procurement Awards")
        
        if hasattr(st.session_state, 'proawards_results') and not st.session_state.proawards_results.empty:
            # Show success message with total count
            total_awards_count = getattr(st.session_state, 'total_awards_count', len(st.session_state.proawards_results))
            
            
            # Show pagination controls for awards results
            current_awards_page = pagination_ui(
                total_awards_count, 
                PAGE_SIZE, 
                key="awards"
            )

            
            # If page changed, update results
            if current_awards_page != st.session_state.awards_page:
                st.session_state.awards_page = current_awards_page
                # Re-fetch data for the new page
                proawards_df, _ = search_proawards(
                    keyword, 
                    page=current_awards_page, 
                    page_size=PAGE_SIZE
                )
                st.session_state.proawards_results = proawards_df
                st.rerun()
            
            # Display the current page of results
            st.dataframe(st.session_state.proawards_results, use_container_width=True)
        else:
            st.error(f"There are no procurement award matches for your selected keyword(s): {keyword}")
    

    if (st.session_state.show_results and not st.session_state.results.empty) or \
       (st.session_state.show_awards and hasattr(st.session_state, 'proawards_results') and not st.session_state.proawards_results.empty):
        
        dfs_to_combine = []
        if not st.session_state.results.empty:
            dfs_to_combine.append(st.session_state.results)
        
        if hasattr(st.session_state, 'proawards_results') and not st.session_state.proawards_results.empty:
            dfs_to_combine.append(st.session_state.proawards_results)
        
        if dfs_to_combine:
            combined_df = pd.concat(dfs_to_combine, ignore_index=True)
            combined_df_filled = combined_df.fillna("N/A")

            csv = combined_df_filled.to_csv(index=False).encode('utf-8')

            st.download_button(
                label="Download Data Report",
                data=csv,
                file_name='combined_data.csv',
                mime='text/csv',
            )

def create_indexes():
    """
    Create database indexes on frequently queried columns to improve search performance.
    This function handles both the main newtable and nycproawards4 tables.
    """
    index_queries = [
        # Primary table indexes (newtable)
        "CREATE INDEX IF NOT EXISTS idx_agency ON newtable (Agency)",
        "CREATE INDEX IF NOT EXISTS idx_method ON newtable (`Procurement Method`)",
        "CREATE INDEX IF NOT EXISTS idx_fiscal_qtr ON newtable (`Fiscal Quarter`)",
        "CREATE INDEX IF NOT EXISTS idx_job_titles ON newtable (`Job Titles`)",
        "CREATE INDEX IF NOT EXISTS idx_services ON newtable (`Services Descrption`(255))",
        
        # Compound indexes for common multi-filter scenarios
        "CREATE INDEX IF NOT EXISTS idx_agency_method ON newtable (Agency, `Procurement Method`)",
        
        # FULLTEXT index for better keyword searching
        "ALTER TABLE newtable ADD FULLTEXT INDEX IF NOT EXISTS ft_services (`Services Descrption`)",
        
        # Procurement awards table indexes
        "CREATE INDEX IF NOT EXISTS idx_award_agency ON nycproawards4 (Agency)",
        "CREATE INDEX IF NOT EXISTS idx_award_title ON nycproawards4 (Title(255))",
        "CREATE INDEX IF NOT EXISTS idx_award_date ON nycproawards4 (`Award Date`)",
        "CREATE INDEX IF NOT EXISTS idx_award_category ON nycproawards4 (Category)",
        
        # FULLTEXT index for the awards table
        "ALTER TABLE nycproawards4 ADD FULLTEXT INDEX IF NOT EXISTS ft_award_title (Title)",
        "ALTER TABLE nycproawards4 ADD FULLTEXT INDEX IF NOT EXISTS ft_award_description (Description)"
    ]
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            for query in index_queries:
                try:
                    cursor.execute(query)
                    print(f"✅ Successfully executed: {query}")
                except mysql.connector.Error as err:
                    if err.errno == 1061:  # Index already exists
                        print(f"ℹ️ Index already exists: {query}")
                        continue
                    elif err.errno == 1831:  # Duplicate FULLTEXT index
                        print(f"ℹ️ FULLTEXT index already exists: {query}")
                        continue
                    else:
                        print(f"✅ Database indexing completed")
            conn.commit()
    
    print("✅ Database indexing completed")

if __name__ == "__main__":
    if not check_password():
        st.stop()
    main()
