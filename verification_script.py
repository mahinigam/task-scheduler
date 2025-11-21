import requests
import time
import json

API_URL = "http://localhost:8000"

def submit_task(priority="Medium", payload={"data": "test"}):
    try:
        response = requests.post(
            f"{API_URL}/tasks",
            json={"priority": priority, "payload": payload, "execution_time": 1.0},
            timeout=5
        )
        if response.status_code == 200:
            print(f"Task submitted: {response.json()['id']} [{priority}]")
            return response.json()['id']
        else:
            print(f"Failed to submit task: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error submitting task: {e}")
        return None

def test_priority():
    print("\n--- Testing Priority Queueing ---")
    # Submit Low priority first, then High
    ids = []
    print("Submitting 5 LOW priority tasks...")
    for i in range(5):
        ids.append(submit_task("Low", {"seq": i, "type": "low"}))
    
    print("Submitting 2 HIGH priority tasks...")
    for i in range(2):
        ids.append(submit_task("High", {"seq": i, "type": "high"}))
        
    print("Check logs to verify High tasks are processed before remaining Low tasks.")

def test_rate_limit():
    print("\n--- Testing Rate Limiting ---")
    print("Spamming 60 requests...")
    success = 0
    blocked = 0
    for i in range(60):
        res = requests.post(f"{API_URL}/tasks", json={"priority": "Low", "payload": {}, "execution_time": 1.0})
        if res.status_code == 200:
            success += 1
        elif res.status_code == 429:
            blocked += 1
    print(f"Success: {success}, Blocked: {blocked}")

if __name__ == "__main__":
    # Wait for services to be ready
    print("Waiting for services to settle...")
    for i in range(30):
        try:
            requests.get(f"{API_URL}/docs", timeout=1)
            print("API is ready!")
            break
        except:
            time.sleep(1)
            print(f"Waiting for API... {i+1}/30")
    else:
        print("API failed to start.")
        exit(1)
    
    test_priority()
    test_rate_limit()
