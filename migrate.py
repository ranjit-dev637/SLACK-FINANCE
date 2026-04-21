from database import engine
from sqlalchemy import text
with engine.connect() as c:
    c.execute(text('ALTER TABLE incomes ADD COLUMN IF NOT EXISTS user_id VARCHAR;'))
    c.commit()
