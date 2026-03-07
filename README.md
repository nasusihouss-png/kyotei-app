# Boat Race Predictor

React (Vite) frontend + Node.js/Express backend.

## 1) Start backend (LAN accessible)

```bash
cd backend
npm install
npm run dev
```

Backend runs on `0.0.0.0:3001` (all local interfaces).

## 2) Start frontend (LAN accessible)

```bash
npm install
npm run dev -- --host
```

This runs Vite with `--host` so other devices can access it on your local network.

## 3) API base URL (frontend)

Create `.env.local` in project root (or copy from `.env.example`):

```env
VITE_API_BASE_URL=http://<YOUR_PC_IP>:3001
```

Frontend is configured to use `VITE_API_BASE_URL` for backend access.

## 4) Find your PC IP address

Windows:

```powershell
ipconfig
```

Use your IPv4 address (example: `192.168.1.20`).

## 5) Open on smartphone

1. Connect phone and PC to the same Wi-Fi.
2. Start backend and frontend as above.
3. Open in phone browser:
   `http://<YOUR_PC_IP>:5173`
4. API endpoint from other devices:
   `http://<YOUR_PC_IP>:3001/api/health`
5. Ensure firewall allows Node.js (ports `5173` and `3001`).

## 6) Public tunnel usage (Cloudflare Tunnel example)

Cloudflare Tunnel (`cloudflared`) must be installed on your PC separately.

Start tunnels in separate terminals:

```bash
cloudflared tunnel --url http://localhost:3001
cloudflared tunnel --url http://localhost:5173
```

Set frontend API target to the backend public URL:

```env
VITE_API_BASE_URL=https://<BACKEND_TUNNEL_DOMAIN>
```

Then restart frontend dev server:

```bash
npm run dev -- --host
```

## 7) Stable public deployment (Vercel + Render)

### Frontend (Vercel)

1. Import this repo into Vercel.
2. Framework preset: `Vite`.
3. Build command: `npm run build`.
4. Output directory: `dist`.
5. Set frontend environment variable in Vercel:

```env
VITE_API_BASE_URL=https://<YOUR_RENDER_BACKEND_DOMAIN>
```

Example:

```env
VITE_API_BASE_URL=https://boat-race-backend.onrender.com
```

### Backend (Render)

1. Create a new Web Service from this repo.
2. Root directory: `backend`.
3. Build command: `npm install`.
4. Start command: `npm run start`.
5. Required/optional backend env vars:
   - `PORT` (provided by Render automatically)
   - `HOST` (optional, default `0.0.0.0`)

Backend health check endpoint:

```text
https://<YOUR_RENDER_BACKEND_DOMAIN>/api/health
```

### Environment variable summary

- Frontend (Vercel): `VITE_API_BASE_URL`
- Backend (Render): `PORT` (auto), `HOST` (optional)
