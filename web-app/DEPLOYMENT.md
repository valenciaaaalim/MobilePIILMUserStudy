# Deployment Guide

This guide covers deployment of the web app to Google Cloud Run with Cloudflare in front.

## Prerequisites

- Google Cloud Platform account with billing enabled
- Cloudflare account
- Domain name (optional, can use Cloud Run provided domain)
- `gcloud` CLI installed and configured
- Docker installed

## Google Cloud Run Deployment

### 1. Set Up GCP Project

```bash
# Set your project ID
export PROJECT_ID=your-project-id
export REGION=us-central1
export SERVICE_NAME=web-app-backend

gcloud config set project $PROJECT_ID
```

### 2. Enable Required APIs

```bash
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

### 3. Build and Push Docker Image

```bash
cd web-app/backend

# Build and push to Container Registry
gcloud builds submit --tag gcr.io/$PROJECT_ID/$SERVICE_NAME

# Or use Artifact Registry (recommended)
gcloud artifacts repositories create web-app-repo \
  --repository-format=docker \
  --location=$REGION

gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/web-app-repo/$SERVICE_NAME
```

### 4. Deploy to Cloud Run

```bash
gcloud run deploy $SERVICE_NAME \
  --image $REGION-docker.pkg.dev/$PROJECT_ID/web-app-repo/$SERVICE_NAME \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --max-instances 10 \
  --min-instances 0 \
  --set-env-vars "GEMINI_API_KEY=your_api_key,DATABASE_URL=sqlite:///tmp/web_app.db,FRONTEND_URL=https://your-domain.com"
```

### 5. Get Cloud Run URL

After deployment, note the service URL:
```bash
gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)'
```

## Cloudflare Configuration

### 1. Add Domain to Cloudflare

1. Log in to Cloudflare dashboard
2. Add your domain (or subdomain)
3. Update nameservers at your domain registrar

### 2. Create DNS Record

Create a CNAME record pointing to your Cloud Run service:
- **Type**: CNAME
- **Name**: `api` (or your subdomain)
- **Target**: `your-service-url.run.app` (from Cloud Run)
- **Proxy status**: Proxied (orange cloud)

### 3. SSL/TLS Configuration

1. Go to **SSL/TLS** settings
2. Set encryption mode to **Full** or **Full (strict)**
3. Enable **Always Use HTTPS**

### 4. WAF Rules

Create WAF rules to protect your API:

1. Go to **Security** > **WAF**
2. Create rules for:
   - Rate limiting (e.g., 100 requests/minute per IP)
   - Block suspicious patterns
   - Geo-blocking if needed

### 5. Origin Settings

1. Go to **Network** settings
2. Enable:
   - **HTTP/2**
   - **HTTP/3 (QUIC)**
   - **0-RTT Connection Resumption**

### 6. Cloudflare Workers (Optional)

For additional security, you can create a Cloudflare Worker to:
- Add custom headers
- Implement additional rate limiting
- Log requests

Example worker:
```javascript
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  // Add security headers
  const response = await fetch(request, {
    cf: {
      cacheEverything: false,
    }
  })
  
  const newHeaders = new Headers(response.headers)
  newHeaders.set('X-Content-Type-Options', 'nosniff')
  newHeaders.set('X-Frame-Options', 'DENY')
  newHeaders.set('X-XSS-Protection', '1; mode=block')
  
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: newHeaders
  })
}
```

## Environment Variables

Set these in Cloud Run:

```bash
gcloud run services update $SERVICE_NAME \
  --update-env-vars "GEMINI_API_KEY=xxx,DATABASE_URL=postgresql://...,FRONTEND_URL=https://your-domain.com,SESSION_COOKIE_SECURE=true"
```

### Required Variables

- `GEMINI_API_KEY`: Your Google Gemini API key
- `DATABASE_URL`: PostgreSQL connection string (for production) or SQLite path
- `FRONTEND_URL`: Your frontend URL for CORS
- `SESSION_COOKIE_SECURE`: Set to `true` in production
- `SECRET_KEY`: Random secret for session management
- `CSRF_SECRET`: Random secret for CSRF protection

### Secrets Management

For sensitive values, use Secret Manager:

```bash
# Create secret
echo -n "your-api-key" | gcloud secrets create gemini-api-key --data-file=-

# Grant access
gcloud secrets add-iam-policy-binding gemini-api-key \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Use in Cloud Run
gcloud run services update $SERVICE_NAME \
  --update-secrets="GEMINI_API_KEY=gemini-api-key:latest"
```

## Database Setup (PostgreSQL)

For production, use Cloud SQL:

```bash
# Create Cloud SQL instance
gcloud sql instances create web-app-db \
  --database-version=POSTGRES_15 \
  --tier=db-f1-micro \
  --region=$REGION

# Create database
gcloud sql databases create webapp --instance=web-app-db

# Create user
gcloud sql users create webapp-user \
  --instance=web-app-db \
  --password=your-secure-password

# Get connection name
gcloud sql instances describe web-app-db --format="value(connectionName)"

# Connect Cloud Run to Cloud SQL
gcloud run services update $SERVICE_NAME \
  --add-cloudsql-instances=CONNECTION_NAME \
  --update-env-vars="DATABASE_URL=postgresql://webapp-user:password@/webapp?host=/cloudsql/CONNECTION_NAME"
```

## Frontend Deployment

### Option 1: Static Hosting (Cloudflare Pages)

1. Build the frontend:
   ```bash
   cd web-app/frontend
   npm run build
   ```

2. Deploy to Cloudflare Pages:
   - Connect your Git repository
   - Set build command: `npm run build`
   - Set output directory: `build`
   - Set environment variable: `REACT_APP_BACKEND_BASE_URL=https://api.your-domain.com`

### Option 2: Cloud Run

Create a Dockerfile for the frontend:

```dockerfile
FROM node:18-alpine as build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

Deploy similarly to backend.

## Monitoring and Logging

### Cloud Run Logs

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME" --limit 50
```

### Cloudflare Analytics

View analytics in Cloudflare dashboard:
- **Analytics** > **Web Traffic**
- **Security** > **Events**

## Health Checks

The service provides a health check endpoint:

```bash
curl https://api.your-domain.com/health
```

Set up uptime monitoring:
- Cloudflare Uptime Monitoring
- Google Cloud Monitoring alerts

## Troubleshooting

### Common Issues

1. **CORS errors**: Ensure `FRONTEND_URL` matches your frontend domain
2. **Database connection errors**: Check Cloud SQL connection configuration
3. **API key errors**: Verify `GEMINI_API_KEY` is set correctly
4. **Timeout errors**: Increase Cloud Run timeout if needed

### Debugging

```bash
# View logs
gcloud run services logs read $SERVICE_NAME --region $REGION

# Check service status
gcloud run services describe $SERVICE_NAME --region $REGION
```

## Cost Optimization

- Use `min-instances=0` to scale to zero
- Use appropriate memory/CPU allocation
- Enable Cloud CDN for static assets
- Use Cloudflare caching for API responses where appropriate
- Monitor usage with Cloud Monitoring

