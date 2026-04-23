import os
import requests
from dotenv import load_dotenv
print("🔥 FILE IS RUNNING")
# Load environment variables from .env
load_dotenv()

def test_slack_token():
    token = os.getenv("SLACK_BOT_TOKEN")

    # Check if token is loaded
    if not token:
        print("❌ SLACK_BOT_TOKEN not found in .env")
        return

    print("🔍 Token loaded:", token[:10] + "...")  # show partial for safety

    headers = {
        "Authorization": f"Bearer {token}"
    }

    try:
        response = requests.get(
            "https://slack.com/api/auth.test",
            headers=headers,
            timeout=15
        )

        data = response.json()

        print("\n🔁 Raw Response:", data)

        if data.get("ok"):
            print("\n✅ Token is VALID")
            print("👤 User:", data.get("user"))
            print("🏢 Team:", data.get("team"))
        else:
            print("\n❌ Token is INVALID")
            print("Error:", data.get("error"))

    except requests.exceptions.RequestException as e:
        print("\n❌ Network/Request Error:", str(e))


if __name__ == "__main__":
    test_slack_token()