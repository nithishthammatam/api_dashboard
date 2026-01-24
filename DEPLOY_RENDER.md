# Deploying Dashboard API to Render

## 1. Push to GitHub
Ensure this `dashboard-api-python` folder is pushed to your GitHub repository.
If it's inside a mono-repo (like `TimeTrace`), you'll configure the **Root Directory** in Render.

## 2. Create Service on Render
1.  Go to **[dashboard.render.com](https://dashboard.render.com)**.
2.  Click **New +** -> **Web Service**.
3.  Connect your GitHub repository.
4.  **Settings**:
    *   **Name**: `dashboard-api-python`
    *   **Root Directory**: `dashboard-api-python` (Important!)
    *   **Runtime**: `Python 3`
    *   **Build Command**: `pip install -r requirements.txt`
    *   **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`

## 3. Environment Variables
You MUST add the following environment variables in the Render Dashboard (under **Environment** tab):

| Key | Value |
| --- | --- |
| `PYTHON_VERSION` | `3.9.18` (or newer) |
| `ALLOWED_KEYS` | *(Copy from your .env)* |
| `FIREBASE_PROJECT_ID` | *(Copy from your .env)* |
| `FIREBASE_CLIENT_EMAIL` | *(Copy from your .env)* |
| `FIREBASE_PRIVATE_KEY` | *(Copy from your .env - including -----BEGIN...)* |
| `FIREBASE_TOKEN_URI` | `https://oauth2.googleapis.com/token` |

> **Note on Private Key**: Render handles newlines correctly. Just paste the entire key block.

## 4. Verify
Once deployed, open your Render URL (e.g., `https://dashboard-api-python.onrender.com/docs`).
You should see the Swagger UI.
