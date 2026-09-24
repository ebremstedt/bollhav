FROM node:22-slim

WORKDIR /app

# The lockfile matters: `npm install` from package.json alone trips npm's
# resolver ("Cannot read properties of null (reading 'edgesOut')") on this
# dependency tree; `npm ci` installs exactly what the lock says.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# Bind on all interfaces and proxy the API to the `backend` service
# (overridden by compose).
ENV VITE_HOST=0.0.0.0
ENV VITE_API_TARGET=http://backend:8137

EXPOSE 5173

CMD ["npm", "run", "dev"]
