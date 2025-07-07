import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime

# Database configuration - Replace with your actual values
DB_CONFIG = {
    'host': 'your_host',
    'database': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'port': 5432
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()

@st.cache_data
def get_unique_areas():
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
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT sub_area FROM field_prompts WHERE area = %s ORDER BY sub_area;", (area,))
        sub_areas = [row[0] for row in cursor.fetchall()]
        return sub_areas
    except Exception as e:
        st.error(f"Error fetching sub areas: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def get_prompts_for_area_subarea(area, sub_area):
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
    st.set_page_config(
        page_title="Field Prompts Manager",
        layout="wide"
    )
    
    st.title("Field Prompts Manager")
    
    with st.sidebar:
        st.header("Database Info")
        stats = get_database_stats()
        if stats:
            st.metric("Total Records", stats['total_records'])
            if stats['last_updated']:
                st.write(f"Last Updated: {stats['last_updated'].strftime('%Y-%m-%d %H:%M')}")
        
        if st.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.header("Select Filters")
        
        areas = get_unique_areas()
        if not areas:
            st.warning("No areas found. Please run data ingestion first.")
            return
        
        selected_area = st.selectbox("Select Area:", areas)
        
        if selected_area:
            sub_areas = get_sub_areas_for_area(selected_area)
            if not sub_areas:
                st.warning(f"No sub areas found for: {selected_area}")
                return
            
            selected_sub_area = st.selectbox("Select Sub Area:", sub_areas)
        else:
            selected_sub_area = None
    
    with col2:
        st.header("Prompts")
        
        if selected_area and selected_sub_area:
            prompts = get_prompts_for_area_subarea(selected_area, selected_sub_area)
            
            if not prompts:
                st.info(f"No prompts found for {selected_area} -> {selected_sub_area}")
            else:
                st.success(f"Found {len(prompts)} prompts")
                
                for record_id, field, prompt, created_at, updated_at in prompts:
                    with st.expander(f"{field}"):
                        
                        col_time1, col_time2 = st.columns(2)
                        with col_time1:
                            st.caption(f"Created: {created_at.strftime('%Y-%m-%d %H:%M')}")
                        with col_time2:
                            st.caption(f"Updated: {updated_at.strftime('%Y-%m-%d %H:%M')}")
                        
                        st.subheader("Current Prompt:")
                        st.text_area(
                            "Current:",
                            value=prompt,
                            height=100,
                            disabled=True,
                            key=f"current_{record_id}"
                        )
                        
                        st.subheader("Edit Prompt:")
                        new_prompt = st.text_area(
                            "New prompt:",
                            value=prompt,
                            height=150,
                            key=f"new_{record_id}"
                        )
                        
                        col_btn1, col_btn2 = st.columns([1, 3])
                        with col_btn1:
                            if st.button("Update", key=f"btn_{record_id}"):
                                if new_prompt.strip() != prompt.strip():
                                    if update_prompt(record_id, new_prompt.strip()):
                                        st.success("Updated successfully")
                                        st.cache_data.clear()
                                        st.rerun()
                                    else:
                                        st.error("Update failed")
                                else:
                                    st.info("No changes detected")
                        
                        with col_btn2:
                            if st.button("Reset", key=f"reset_{record_id}"):
                                st.rerun()
        else:
            st.info("Please select both Area and Sub Area")

if __name__ == "__main__":
    main()
