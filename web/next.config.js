const path = require('path');
// Load shared root .env so NEXT_PUBLIC_* variables are available in build/runtime
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: { appDir: true },
  reactStrictMode: true,
};

module.exports = nextConfig;
