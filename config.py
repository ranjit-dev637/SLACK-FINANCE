import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_anon_key: str = os.getenv("SUPABASE_ANON_KEY", "")
    supabase_service_role_key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    database_url: str = os.getenv("DATABASE_URL", "")
    slack_bot_token: str = os.getenv("SLACK_BOT_TOKEN", "")
    slack_signing_secret: str = os.getenv("SLACK_SIGNING_SECRET", "")
    google_service_account_json: str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    google_drive_folder_income: str = os.getenv("GOOGLE_DRIVE_FOLDER_INCOME", "")
    google_drive_folder_expense: str = os.getenv("GOOGLE_DRIVE_FOLDER_EXPENSE", "")
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")

settings = Settings()
