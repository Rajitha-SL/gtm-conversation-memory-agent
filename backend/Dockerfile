# Stage 1: Build dependencies and generate Prisma Client
FROM node:20-alpine AS builder

WORKDIR /app

# Install build dependencies (libc6-compat is recommended for Prisma on Alpine)
RUN apk add --no-cache libc6-compat

# Copy package files
COPY package*.json ./
COPY prisma ./prisma/

# Install all dependencies (including devDependencies for prisma)
RUN npm ci

# Generate Prisma Client
RUN npx prisma generate

# Stage 2: Production runtime image
FROM node:20-alpine AS runner

WORKDIR /app

RUN apk add --no-cache libc6-compat

ENV NODE_ENV=production

# Copy package files
COPY package*.json ./

# Install production-only dependencies
RUN npm ci --only=production

# Copy generated Prisma Client from builder stage
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma
COPY --from=builder /app/node_modules/@prisma ./node_modules/@prisma

# Copy application files
COPY . .

EXPOSE 3000

# Default start command (can be overridden in compose)
CMD ["node", "server.js"]
