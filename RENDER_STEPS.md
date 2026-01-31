# Step-by-Step Render Deployment Guide

Since you have selected **"New Web Service"** on Render, follow these exact steps:

## 1. Connect Repository
*   You should see a list of your GitHub repositories.
*   Find **`Cognera_Api_Dashboard`** and click **Connect**.
    *   *If you don't see it, click "Configure account" to grant Render access to this specific repo.*

## 2. Configure Service
Fill in the form with these values:

*   **Name**: `dashboard-api-python` (or any name you like)
*   **Region**: Select the one closest to you (e.g., `Singapore` or `Frankfurt`).
*   **Branch**: `main`
*   **Root Directory**: Leave this **EMPTY** (since we pushed the code to the root of this repo).
*   **Runtime**: **Python 3**
*   **Build Command**: `pip install -r requirements.txt`
*   **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
*   **Instance Type**: **Free**

## 3. Environment Variables (Critical!)
Scroll down to the **"Environment Variables"** section (or "Advanced").
You need to add **5 variables** manually. Copy these from your local `.env` file:

1.  **key**: `PYTHON_VERSION`
    **value**: `3.9.18`
2.  **key**: `ALLOWED_KEYS`
    **value**: *(Paste your API Key)*
3.  **key**: `FIREBASE_PROJECT_ID`
    **value**: `cognera-eddfe`
4.  **key**: `FIREBASE_CLIENT_EMAIL`
    **value**: `firebase-adminsdk-fbsvc@cognera-eddfe.iam.gserviceaccount.com`
5.  **key**: `FIREBASE_PRIVATE_KEY`
    **value**: *(Paste the ENTIRE private key starting with `-----BEGIN PRIVATE KEY-----`)*
6.  **key**: `FIREBASE_TOKEN_URI`
    **value**: `https://oauth2.googleapis.com/token`

## 4. Deploy
*   Click **Create Web Service**.
*   Wait for the logs to show "Your service is live".
*   Your URL will look like `https://dashboard-api-python.onrender.com`.

## 5. Verify
*   Go to `https://<YOUR-URL>/docs` to see the Swagger UI.
*   Authorize with your API Key and test!
