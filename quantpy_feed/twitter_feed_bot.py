import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime

# Import your DB_CONFIG here
# from your_config import DB_CONFIG

def get_db_connection():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()

@st.cache_data
def get_unique_areas():
    """Get all unique areas from database"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT area FROM field_prompts ORDER BY area;")
        areas = [row[0] for row in cursor.fetchall()]
        return areas
    except Exception as e:
        st.error(f"Error fetching areas: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

@st.cache_data
def get_sub_areas_for_area(area):
    """Get all sub areas for a given area"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT sub_area 
            FROM field_prompts 
            WHERE area = %s 
            ORDER BY sub_area;
        """, (area,))
        sub_areas = [row[0] for row in cursor.fetchall()]
        return sub_areas
    except Exception as e:
        st.error(f"Error fetching sub areas: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_prompts_for_area_subarea(area, sub_area):
    """Get all prompts for a given area and sub area"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, field, prompt, created_at, updated_at
            FROM field_prompts 
            WHERE area = %s AND sub_area = %s
            ORDER BY field;
        """, (area, sub_area))
        
        results = cursor.fetchall()
        return results
    except Exception as e:
        st.error(f"Error fetching prompts: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def update_prompt(record_id, new_prompt):
    """Update a specific prompt"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE field_prompts 
            SET prompt = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (new_prompt, record_id))
        
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error updating prompt: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def get_database_stats():
    """Get database statistics for sidebar"""
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM field_prompts;")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT MAX(updated_at) FROM field_prompts;")
        last_updated = cursor.fetchone()[0]
        
        return {
            'total_records': total_records,
            'last_updated': last_updated
        }
    except Exception as e:
        st.error(f"Error getting database stats: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def main():
    """Main Streamlit application"""
    
    st.set_page_config(
        page_title="Field Prompts Manager",
        page_icon="📝",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .prompt-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="main-header">Field Prompts Manager</div>', unsafe_allow_html=True)
    
    with st.sidebar:
        st.header("Database Info")
        
        stats = get_database_stats()
        if stats:
            st.metric("Total Records", stats['total_records'])
            if stats['last_updated']:
                st.write(f"**Last Updated:** {stats['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        st.divider()
        
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.header("Select Filters")
        
        areas = get_unique_areas()
        if not areas:
            st.warning("No areas found in database. Please run data ingestion first.")
            st.stop()
        
        selected_area = st.selectbox(
            "Select Area:",
            areas,
            key="area_selector"
        )
        
        if selected_area:
            sub_areas = get_sub_areas_for_area(selected_area)
            if not sub_areas:
                st.warning(f"No sub areas found for area: {selected_area}")
                st.stop()
            
            selected_sub_area = st.selectbox(
                "Select Sub Area:",
                sub_areas,
                key="sub_area_selector"
            )
        else:
            selected_sub_area = None
    
    with col2:
        st.header("Prompts")
        
        if selected_area and selected_sub_area:
            prompts = get_prompts_for_area_subarea(selected_area, selected_sub_area)
            
            if not prompts:
                st.info(f"No prompts found for {selected_area} -> {selected_sub_area}")
            else:
                st.success(f"Found {len(prompts)} prompts for **{selected_area}** -> **{selected_sub_area}**")
                
                for i, (record_id, field, prompt, created_at, updated_at) in enumerate(prompts):
                    
                    with st.expander(f"{field}", expanded=False):
                        
                        col_time1, col_time2 = st.columns(2)
                        with col_time1:
                            st.caption(f"Created: {created_at.strftime('%Y-%m-%d %H:%M')}")
                        with col_time2:
                            st.caption(f"Updated: {updated_at.strftime('%Y-%m-%d %H:%M')}")
                        
                        st.subheader("Current Prompt:")
                        st.text_area(
                            "Current content:",
                            value=prompt,
                            height=100,
                            disabled=True,
                            key=f"current_prompt_{record_id}"
                        )
                        
                        st.subheader("Edit Prompt:")
                        
                        new_prompt = st.text_area(
                            "Enter new prompt:",
                            value=prompt,
                            height=150,
                            key=f"new_prompt_{record_id}",
                            help="Edit the prompt content here"
                        )
                        
                        col_btn1, col_btn2 = st.columns([1, 3])
                        with col_btn1:
                            if st.button(f"Update", key=f"update_btn_{record_id}"):
                                if new_prompt.strip() != prompt.strip():
                                    with st.spinner("Updating..."):
                                        if update_prompt(record_id, new_prompt.strip()):
                                            st.success("Prompt updated successfully")
                                            st.cache_data.clear()
                                            st.rerun()
                                        else:
                                            st.error("Failed to update prompt")
                                else:
                                    st.info("No changes detected")
                        
                        with col_btn2:
                            if st.button(f"Reset", key=f"reset_btn_{record_id}"):
                                st.rerun()
                        
                        st.divider()
        else:
            st.info("Please select both Area and Sub Area to view prompts")

if __name__ == "__main__":
    main()
