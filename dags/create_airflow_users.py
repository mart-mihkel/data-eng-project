from airflow.models import DagBag
from airflow.security import permissions
from airflow.www.app import create_app
from airflow import settings
from airflow.models import User, Role

# Airflow roles and permissions
roles_and_users = [
    {
        "username": "admin",
        "firstname": "Admin",
        "lastname": "User",
        "role": "Admin",
        "email": "admin@example.com",
        "password": "admin_password",
    },
    {
        "username": "viewer",
        "firstname": "Viewer",
        "lastname": "User",
        "role": "Viewer",
        "email": "viewer@example.com",
        "password": "viewer_password",
    },
    {
        "username": "editor",
        "firstname": "Editor",
        "lastname": "User",
        "role": "User",
        "email": "editor@example.com",
        "password": "editor_password",
    },
]

def create_users():
    session = settings.Session()

    for user in roles_and_users:
        # Check if the user already exists
        existing_user = session.query(User).filter(User.username == user["username"]).first()
        if not existing_user:
            role = session.query(Role).filter(Role.name == user["role"]).first()
            new_user = User(
                username=user["username"],
                email=user["email"],
                first_name=user["firstname"],
                last_name=user["lastname"],
                password=user["password"],
                roles=[role]
            )
            session.add(new_user)
            print(f"User {user['username']} created with role {user['role']}")
        else:
            print(f"User {user['username']} already exists.")
    
    session.commit()
    session.close()

if __name__ == "__main__":
    create_users()
