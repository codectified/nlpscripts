from notion_client import Client
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Replace this or set it as an environment variable
PARENT_PAGE_ID = os.getenv("NOTION_PARENT_PAGE_ID", "21af9c161f3a808a9a0acef264bc39ca")

notion = Client(auth=os.getenv("NOTION_API_TOKEN"))


sections = {
    "🧠 Mindroots – Architecture & Design": [
        ("Expand Query Types", "Done", "Add support for form and grammatical queries."),
        ("Modularize Prompt Chains", "Done", "Split semantic, translation, and Cypher logic."),
        ("Start External API Testing", "Backlog", "Lay groundwork for cross-linguistic comparison."),
    ],
    "📊 Business – Strategy & Marketing": [
        ("Collect Use Cases", "Done", "Apply GraphRAG to other domains."),
        ("LinkedIn Outreach Strategy", "Backlog", "Develop custom GPT showcase."),
    ],
    "⚙️ Infra – DevOps & Deployments": [
        ("Backend Overhaul", "Backlog", "Complete link routing and API modularity."),
        ("Morph Form Audit", "Backlog", "Clean up LLM-generated morphological form noise."),
    ],
    "🧪 Experiments – LLM / GPT": [
        ("Define Validation Workflow", "Backlog", "Outline steps for GPT-assisted crowdsourcing."),
        ("Build Flagging System", "Backlog", "Enable review-based edits and progress tracking."),
    ],
    "📝 Writing & Public Sharing": [
        ("Write GraphRAG Explainer", "Backlog", "Introduce the concept in blog form."),
        ("Publish Orientation Deck", "Backlog", "Overview of system & vision."),
    ],
    "💼 Admin – Bills, Finances, Legal": [
        ("LLM Subscription Audit", "Backlog", "Track usage and clean up spend."),
    ],
    "🧭 Orientation – Priority / Vision Planning": [
        ("Map Project Milestones", "Backlog", "Create high-level map across Option / Frame / Lab."),
        ("Clarify Strategic Priorities", "Backlog", "Determine what’s foundational vs. stretch."),
    ]
}

# --- HELPERS ---
def create_section_and_database(section_title, tasks):
    print(f"\n📁 Creating section: {section_title}")
    section_page = notion.pages.create(
        parent={"page_id": PARENT_PAGE_ID},
        properties={
            "title": [{"type": "text", "text": {"content": section_title}}]
        }
    )
    section_id = section_page["id"]
    print(f"✅ Created page: {section_title} → {section_id}")

    print("📦 Creating task database...")
    db = notion.databases.create(
        parent={"page_id": section_id},
        title=[{"type": "text", "text": {"content": "Tasks"}}],
        properties={
            "Task": {"title": {}},
            "Status": {"select": {"options": [
                {"name": "Backlog", "color": "gray"},
                {"name": "In Progress", "color": "blue"},
                {"name": "Done", "color": "green"}
            ]}},
            "Notes": {"rich_text": {}}
        }
    )
    db_id = db["id"]
    print(f"✅ Created task database in {section_title}")

    for (task, status, note) in tasks:
        clean_text = task.strip()
        try:
            notion.pages.create(
                parent={"database_id": db_id},
                properties={
                    "Task": {
                        "title": [{"type": "text", "text": {"content": clean_text}}]
                    },
                    "Status": {"select": {"name": status}},
                    "Notes": {
                        "rich_text": [{"type": "text", "text": {"content": note}}] if note else []
                    }
                }
            )
            print(f"  ➕ Added task: {clean_text} [{status}]")
        except Exception as e:
            print(f"❌ Failed to create task: {clean_text}")
            print(e)
        time.sleep(0.3)  # small delay to avoid rate limits

# --- RUN SETUP ---
for section, task_list in sections.items():
    create_section_and_database(section, task_list)