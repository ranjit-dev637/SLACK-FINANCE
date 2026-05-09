import re

def refactor_modals():
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Replace the old /income command handler
    # Find @slack_app.command("/income") to # ============================== or @slack_app.view
    pattern_income = r'@slack_app\.command\("/income"\).*?(?=@slack_app\.command\("/expense"\)|# ==============================|@slack_app\.view)'
    replacement_income = """@slack_app.command("/income")
def handle_income_command(ack, body, client):
    ack()
    client.views_open(
        trigger_id=body["trigger_id"],
        view=get_income_modal()
    )

"""
    content = re.sub(pattern_income, replacement_income, content, flags=re.DOTALL)
    
    # Do the same for /expense, income_modal view, expense_modal view
    
    # Let's just append the new modal definitions at the top or bottom of the slack block.
    # To be safe, I'll just replace the whole section from @slack_app.command("/income") down to the start of the FastAPI routes.
    
    # Find @slack_app.command("/income")
    idx_start = content.find('@slack_app.command("/income")')
    if idx_start == -1:
        # Maybe it's not found? Let's check.
        pass
    
    idx_end = content.find('@app.post("/expense"', idx_start)
    if idx_end == -1:
        # fallback
        idx_end = content.find('@app.post("/slack/events"', idx_start)
        
    if idx_start != -1 and idx_end != -1:
        # replace the whole chunk
        pass

if __name__ == '__main__':
    refactor_modals()
