import os
import json
import base64
import requests
import time
from concurrent.futures import ThreadPoolExecutor

def process_json_files(folder_path, output_file, failed_file):
    """
    Process all JSON files in a folder, extract thumbnail_url, convert to base64,
    and save the results in a single JSON file with associated product IDs.
    Process one item at a time and write to output file incrementally.
    """
    # Create or clear the output file
    with open(output_file, 'w', encoding='utf-8') as out_file:
        out_file.write('[\n')  # Start JSON array
    
    # List to track failed IDs
    failed_ids = []
    
    # Get all JSON files in the folder
    json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
    print(f"Found {len(json_files)} JSON files to process")
    
    # Total files counter for progress tracking
    total_files = len(json_files)
    processed_files = 0
    processed_items = 0
    is_first_item = True
    
    for file_name in json_files:
        file_path = os.path.join(folder_path, file_name)
        
        try:
            # Load JSON data
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Process each item in the JSON file
            if isinstance(data, list):
                for item in data:
                    result = process_item(item)
                    write_to_output(result, output_file, is_first_item)
                    
                    # Update first item flag
                    if is_first_item and result:
                        is_first_item = False
                    
                    # Add to failed list if processing failed
                    if not result and item.get('id'):
                        failed_ids.append(item.get('id'))
                    
                    processed_items += 1
                    if processed_items % 10 == 0:
                        print(f"Processed {processed_items} items so far")
            else:
                # Handle the case where JSON file contains a single object
                result = process_item(data)
                write_to_output(result, output_file, is_first_item)
                
                # Update first item flag
                if is_first_item and result:
                    is_first_item = False
                
                # Add to failed list if processing failed
                if not result and data.get('id'):
                    failed_ids.append(data.get('id'))
                
                processed_items += 1
        
        except Exception as e:
            print(f"Error processing file {file_name}: {str(e)}")
        
        # Update progress
        processed_files += 1
        print(f"Processed {processed_files}/{total_files} files - {processed_items} items processed")
    
    # Close the JSON array in the output file
    with open(output_file, 'a', encoding='utf-8') as out_file:
        out_file.write('\n]')
    
    # Save failed IDs to a separate file
    with open(failed_file, 'w', encoding='utf-8') as fail_file:
        json.dump({"failed_ids": failed_ids}, fail_file, ensure_ascii=False, indent=2)
    
    print(f"Completed! Processed {processed_items} items")
    print(f"Successfully processed items saved to {output_file}")
    print(f"Failed to process {len(failed_ids)} items, IDs saved to {failed_file}")

def process_item(item):
    """Process a single JSON item to extract ID and thumbnail_url"""
    try:
        # Extract product ID
        product_id = item.get('id')
        if not product_id:
            return None
        
        # Extract thumbnail URL
        thumbnail_url = item.get('thumbnail_url')
        if not thumbnail_url:
            return None
        
        # Download image with retry logic
        image_base64 = download_with_retry(thumbnail_url)
        if not image_base64:
            print(f"Failed to download image for product ID {product_id} after retries")
            return None
        
        # Thêm prefix "data:image/" vào chuỗi base64
        result = {
            'id': product_id,
            'image_base64': f"data:image/jpeg;base64,{image_base64}"
        }
        
        return result
    
    except Exception as e:
        print(f"Error processing item with ID {item.get('id', 'unknown')}: {str(e)}")
        return None

def download_with_retry(url, max_retries=3, retry_delay=2):
    """Download image from URL with retry logic"""
    retries = 0
    
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=100)
            response.raise_for_status()
            
            # Convert the image to base64
            image_data = base64.b64encode(response.content).decode('utf-8')
            return image_data
        
        except Exception as e:
            retries += 1
            if retries < max_retries:
                print(f"Error downloading image from {url}: {str(e)}. Retrying in {retry_delay} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"Failed to download image from {url} after {max_retries} attempts: {str(e)}")
                return None

def write_to_output(result, output_file, is_first_item):
    """Write a single result to the output file"""
    if not result:
        return
    
    with open(output_file, 'a', encoding='utf-8') as out_file:
        if not is_first_item:
            out_file.write(',\n')
        # Sử dụng indent=4 để format JSON đẹp hơn
        json_str = json.dumps(result, ensure_ascii=False, indent=4)
        out_file.write(json_str)

if __name__ == "__main__":
    # Input folder containing JSON files
    input_folder = "data"
    
    # Output file
    output_file = "map_image_base64.json"
    
    # Failed IDs file
    failed_file = "fail_map.json"
    
    # Process all files
    process_json_files(input_folder, output_file, failed_file)