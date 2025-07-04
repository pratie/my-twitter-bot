import time
import logging
from quantpy_feed.twitter_feed_bot import run_quantpy_feed_bot
from apscheduler.schedulers.background import BackgroundScheduler

# set up logging to file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%y-%m-%d %H:%M",
    filename="twitter-bot.log",
    filemode="a",
)

if __name__ == "__main__":
    # Creates a default Background Scheduler
    sched = BackgroundScheduler(daemon=True)

    # run once to publish
    run_quantpy_feed_bot()

    # then add to scheduler
    sched.add_job(run_quantpy_feed_bot, "interval", hours=12)

    # start scheduler
    sched.start()

    while True:
        time.sleep(60)


############################################################################################################
import pandas as pd
import re

def extract_entities_simple(data: str):
    """
    Extract entity names and their full prompts from the data string
    Only returns entity_name and entity_prompt
    """
    entities = []
    
    # Split content into main sections starting with number at beginning of line
    # This pattern looks for numbers at the start of a line (main entities)
    main_sections = re.split(r'\n(?=\d+\.\s+[A-Z])', data)
    
    for section in main_sections:
        if not section.strip():
            continue
            
        # Extract the main entity number and name from the first line
        first_line_match = re.match(r'^(\d+)\.\s+(.+?)(?:\n|$)', section, re.MULTILINE)
        
        if not first_line_match:
            continue
            
        entity_number = first_line_match.group(1)
        entity_name = first_line_match.group(2).strip()
        
        # Clean up entity name (remove extra spaces)
        entity_name = re.sub(r'\s+', ' ', entity_name).strip()
        
        # The full prompt is the entire section (including the entity name line)
        entity_prompt = section.strip()
        
        entities.append({
            'entity_name': entity_name,
            'entity_prompt': entity_prompt
        })
    
    return entities

def save_entities_to_csv(entities, filename='entities.csv'):
    """Save entities to CSV file"""
    df = pd.DataFrame(entities)
    df.to_csv(filename, index=False)
    print(f"Saved {len(entities)} entities to {filename}")
    return df

# Usage example:
# Assuming you have your data in a variable called 'data'

# Extract entities
entities = extract_entities_simple(data)

# Save to CSV
df = save_entities_to_csv(entities)

# Preview results
print("Preview of extracted entities:")
for i, entity in enumerate(entities[:3]):  # Show first 3
    print(f"\n{i+1}. Entity Name: {entity['entity_name']}")
    print(f"   Prompt Preview: {entity['entity_prompt'][:100]}...")

print(f"\nTotal entities extracted: {len(entities)}")
print(f"CSV saved with columns: {list(df.columns)}")


