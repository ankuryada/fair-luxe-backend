
import mongoose from "mongoose";

const uri = process.env.MONGODB_URI;

if (!uri) {
  throw new Error("MONGODB_URI is missing from .env.local");
}

const globalCache = globalThis as typeof globalThis & {
  mongooseCache?: {
    connection: typeof mongoose | null;
    promise: Promise<typeof mongoose> | null;
  };
};

const cache = (globalThis.mongooseCache ??= {
  connection: null,
  promise: null,
});

export async function connectDB() {
  if (cache.connection) return cache.connection;

  cache.promise ??= mongoose.connect(uri);

  try {
    cache.connection = await cache.promise;
    return cache.connection;
  } catch (error) {
    cache.promise = null;
    throw error;
  }
}
