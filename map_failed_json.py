import os
import json
import base64
import requests
import time

def retry_failed_ids(folder_path, failed_file, output_file):
    with open(failed_file, 'r', encoding='utf-8') as f:
        failed_data = json.load(f)
        failed_ids = set(failed_data.get("failed_ids", []))

    print(f"Retrying {len(failed_ids)} failed IDs...")

    # Collect items with matching IDs
    id_to_item = {}
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".json"):
            file_path = os.path.join(folder_path, file_name)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        pid = item.get("id")
                        if pid in failed_ids:
                            id_to_item[pid] = item
                except Exception as e:
                    print(f"Error reading {file_name}: {e}")

    # Prepare data to append
    results = []
    still_failed = []
    for pid in failed_ids:
        item = id_to_item.get(pid)
        if not item:
            still_failed.append(pid)
            continue
        result = process_item(item)
        if result:
            results.append(result)
        else:
            still_failed.append(pid)

    # Reopen output file and insert before closing ]
    if results:
        with open(output_file, 'r+', encoding='utf-8') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell() - 1
            while pos > 0 and f.read(1) != ']':
                pos -= 1
                f.seek(pos, os.SEEK_SET)
            f.seek(pos, os.SEEK_SET)
            f.truncate()

            # Write new entries
            f.write(',\n' + ',\n'.join(json.dumps(r, ensure_ascii=False, indent=4) for r in results))
            f.write('\n]')

    # Save remaining failed IDs
    with open(failed_file, 'w', encoding='utf-8') as f:
        json.dump({"failed_ids": still_failed}, f, ensure_ascii=False, indent=2)

    print(f"Retried done. {len(results)} new items appended. {len(still_failed)} still failed.")

def process_item(item):
    """Same as before"""
    try:
        product_id = item.get('id')
        thumbnail_url = item.get('thumbnail_url')
        if not product_id or not thumbnail_url:
            return None
        image_base64 = download_with_retry(thumbnail_url)
        if not image_base64:
            return None
        return {
            'id': product_id,
            'image_base64': f"data:image/jpeg;base64,{image_base64}"
        }
    except:
        return None

def download_with_retry(url, max_retries=3, retry_delay=2):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return base64.b64encode(response.content).decode('utf-8')
        except:
            time.sleep(retry_delay)
            retries += 1
    return None

if __name__ == "__main__":
    retry_failed_ids(folder_path="data", failed_file="fail_map.json", output_file="map_image_base64.json")
