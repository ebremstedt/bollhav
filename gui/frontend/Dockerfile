FROM node:22-slim

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

# Bind on all interfaces and proxy the API to the `backend` service
# (overridden by compose).
ENV VITE_HOST=0.0.0.0
ENV VITE_API_TARGET=http://backend:8137

EXPOSE 5173

CMD ["npm", "run", "dev"]
