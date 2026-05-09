import re

def apply_routing():
    with open('main.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Fix responses in /slack/events
    content = content.replace('return {"status": "duplicate ignored"}', 'return Response(status_code=200)')
    content = content.replace('return {"status": "ok"}', 'return Response(status_code=200)')

    # Add /slack/commands and /slack/interactive routes if they don't exist
    if '@app.post("/slack/commands")' not in content:
        idx = content.find('@app.post("/slack/events")')
        if idx != -1:
            new_routes = """@app.post("/slack/commands")
async def slack_commands(request: Request):
    return await handler.handle(request)

@app.post("/slack/interactive")
async def slack_interactive(request: Request):
    return await handler.handle(request)

"""
            content = content[:idx] + new_routes + content[idx:]

    with open('main.py', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == '__main__':
    apply_routing()
    print("Routing updated successfully.")
