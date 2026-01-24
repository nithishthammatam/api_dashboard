# TimeTrace Dashboard API (Python Version)

This is a Python rewrite of the Dashboard API using **FastAPI**.
It provides the same endpoints as the Node.js version but includes interactive documentation.

## Features
- **FastAPI**: High performance, easy to use.
- **Interactive Docs**: Swagger UI available at `/docs`.
- **Firebase Admin**: Secure access to Firestore.
- **Railway Ready**: Configured for easy deployment.

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Environment Variables**:
    Ensure you have a `.env` file with the following:
    ```env
    ALLOWED_KEYS=your_api_key_1,your_api_key_2
    FIREBASE_PROJECT_ID=your_project_id
    FIREBASE_PRIVATE_KEY=your_private_key
    FIREBASE_CLIENT_EMAIL=your_client_email
    ALLOWED_ORIGINS=*
    ```

3.  **Run Locally**:
    ```bash
    uvicorn main:app --reload
    ```
    Access the API at `http://localhost:8000`.
    Docs at `http://localhost:8000/docs`.

## Deployment (Railway)

### Option 1: Railway CLI
1.  Initialize project: `railway init`
2.  Deploy: `railway up`

### Option 2: GitHub
1.  Push this directory to GitHub.
2.  Connect to Railway.
3.  Set the Root Directory to `dashboard-api-python`.

## Endpoints
- `GET /health`
- `GET /api/dashboard/users`
- `GET /api/dashboard/screentime`
- `GET /api/dashboard/sessions`
