const path = require('path');
// Explicitly target the root directory path to ensure environment variables load accurately
require('dotenv').config({ path: path.resolve(__dirname, '.env') });
const { defineConfig } = require('@prisma/config');

module.exports = defineConfig({
  schema: 'prisma/schema.prisma',
  datasource: {
    url: process.env.DATABASE_URL
  }
});