#!/usr/bin/env python3
"""
Script to update the group configuration with the correct Market Mercenaries identifier
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

def update_group_config(group_identifier):
    """Update the config file with the correct group identifier"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'btc_sentiment', 'config', 'config.py')
    
    # Read the current config
    with open(config_path, 'r') as f:
        content = f.read()
    
    # Update the TELEGRAM_GROUPS list
    old_groups_section = '''TELEGRAM_GROUPS: List[str] = [
        "@marketmercenaries",  # Try username format first
        "Market Mercenaries",   # Display name
        "marketmercenaries"     # Username without @
    ]'''
    
    new_groups_section = f'''TELEGRAM_GROUPS: List[str] = [
        "{group_identifier}"
    ]'''
    
    # Replace the section
    updated_content = content.replace(old_groups_section, new_groups_section)
    
    # Write back to file
    with open(config_path, 'w') as f:
        f.write(updated_content)
    
    print(f"✅ Updated config to use: {group_identifier}")
    print("🔄 Please restart your notebook kernel to apply changes")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python update_group_config.py <group_identifier>")
        print("Example: python update_group_config.py '@marketmercenaries'")
        sys.exit(1)
    
    group_identifier = sys.argv[1]
    update_group_config(group_identifier)