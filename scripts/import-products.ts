
import path from "node:path";
import fs from "node:fs/promises";
import { createReadStream } from "node:fs";
import dotenv from "dotenv";
import mongoose from "mongoose";
import { GridFSBucket, ObjectId } from "mongodb";
import { pathToFileURL } from "node:url";
// Load the backend's .env.local file.
dotenv.config({ path: path.resolve(process.cwd(), ".env.local") });

const FRONTEND_ROOT = path.resolve(process.cwd(), "../fair-luxe");
const DATA_ROOT = path.join(FRONTEND_ROOT, "src", "data");
const IMAGE_ROOT = path.join(FRONTEND_ROOT, "public", "images");

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
  throw new Error("MONGODB_URI is missing from backend/.env.local");
}

type Product = Record<string, any> & {
  id: number | string;
  name: string;
  image?: string;
  images?: string[];
  type: string;
};

const mimeTypes: Record<string, string> = {
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".png": "image/png",
  ".webp": "image/webp",
  ".avif": "image/avif",
  ".gif": "image/gif",
};

function getImageDiskPath(publicPath: string): string {
  // Example:
  // /images/products/women/201-front.jpg
  // becomes public/images/women/201-front.jpg
  let relativePath = publicPath
    .replace(/^\/+/, "")
    .replace(/^images\//, "")
    .replace(/^products\//, "");

  // The sale catalog uses /images/products/sale/,
  // but the actual folder is public/images/saleproduct/.
  relativePath = relativePath.replace(/^sale\//, "saleproduct/");

  return path.resolve(IMAGE_ROOT, relativePath);
}

// async function loadProducts(): Promise<Product[]> {
//   const [
//     womenModule,
//     menModule,
//     newArrivalsModule,
//     accessoriesModule,
//     saleModule,
//   ] = await Promise.all([
//     import(path.join(DATA_ROOT, "womenProducts.ts")),
//     import(path.join(DATA_ROOT, "menProducts.ts")),
//     import(path.join(DATA_ROOT, "newArrivals.ts")),
//     import(path.join(DATA_ROOT, "accessories.ts")),
//     import(path.join(DATA_ROOT, "saleProducts.ts")),
//   ]);

//   return [
//     ...womenModule.womenProducts.map((p: Product) => ({
//       ...p,
//       type: "women",
//       sizes: p.sizes ?? p.size ?? [],
//     })),

//     ...menModule.menProducts.map((p: Product) => ({
//       ...p,
//       type: "men",
//     })),

//     ...newArrivalsModule.default.map((p: Product) => ({
//       ...p,
//       type: "new",
//       sizes: p.sizes ?? ["XS", "S", "M", "L", "XL"],
//     })),

//     ...accessoriesModule.accessories.map((p: Product) => ({
//       ...p,
//       type: "accessories",
//     })),

//     ...saleModule.saleProducts.map((p: Product) => ({
//       ...p,
//       type: "sale",
//       gender: p.category,
//     })),
//   ];
// }


async function loadProducts(): Promise<Product[]> {
  const importFile = async (filename: string) => {
    const absolutePath = path.resolve(DATA_ROOT, filename);

    // Convert C:\... Windows paths into file:///C:/... URLs.
    const fileUrl = pathToFileURL(absolutePath).href;

    return import(fileUrl);
  };

  const [
    womenModule,
    menModule,
    newArrivalsModule,
    accessoriesModule,
    saleModule,
  ] = await Promise.all([
    importFile("womenProducts.ts"),
    importFile("menProducts.ts"),
    importFile("newArrivals.ts"),
    importFile("accessories.ts"),
    importFile("saleProducts.ts"),
  ]);

  return [
    ...womenModule.womenProducts.map((p: Product) => ({
      ...p,
      type: "women",
      sizes: p.sizes ?? p.size ?? [],
    })),

    ...menModule.menProducts.map((p: Product) => ({
      ...p,
      type: "men",
    })),

    ...newArrivalsModule.default.map((p: Product) => ({
      ...p,
      type: "new",
      sizes: p.sizes ?? ["XS", "S", "M", "L", "XL"],
    })),

    ...accessoriesModule.accessories.map((p: Product) => ({
      ...p,
      type: "accessories",
    })),

    ...saleModule.saleProducts.map((p: Product) => ({
      ...p,
      type: "sale",
      gender: p.category,
    })),
  ];
}


async function uploadImage(
  bucket: GridFSBucket,
  db: NonNullable<typeof mongoose.connection.db>,
  publicPath: string,
): Promise<string> {
  const absolutePath = getImageDiskPath(publicPath);

  // Reuse an image already uploaded from this source path.
  const existing = await db.collection("productImages.files").findOne({
    "metadata.sourcePath": publicPath,
  });

  if (existing?._id) {
    return existing._id.toString();
  }

  await fs.access(absolutePath);

  const extension = path.extname(absolutePath).toLowerCase();
  const fileId = new ObjectId();

  const upload = bucket.openUploadStreamWithId(
    fileId,
    path.basename(absolutePath),
    {
      contentType: mimeTypes[extension] ?? "application/octet-stream",
      metadata: { sourcePath: publicPath },
    },
  );

  await new Promise<void>((resolve, reject) => {
    const input = createReadStream(absolutePath);

    input.on("error", reject);
    upload.on("error", reject);
    upload.on("finish", resolve);

    input.pipe(upload);
  });

  return fileId.toString();
}

async function main() {
  await mongoose.connect(MONGODB_URI!);

  const db = mongoose.connection.db;

  if (!db) {
    throw new Error("Could not access the MongoDB database.");
  }

  const bucket = new GridFSBucket(db, {
    bucketName: "productImages",
  });

  const productsCollection = db.collection("products");
  const products = await loadProducts();

  let importedProducts = 0;
  let uploadedImages = 0;
  let missingImages = 0;

  console.log(`Found ${products.length} products.`);

  for (const product of products) {
    const imagePaths = [
      product.image,
      ...(Array.isArray(product.images) ? product.images : []),
    ].filter((value): value is string => Boolean(value));

    const uniqueImagePaths = [...new Set(imagePaths)];
    const imageRefs: {
      sourcePath: string;
      fileId: string;
      url: string;
    }[] = [];

    for (const publicPath of uniqueImagePaths) {
      try {
        const absolutePath = getImageDiskPath(publicPath);

        // Check that the referenced local image exists.
        await fs.access(absolutePath);

        const previouslyUploaded = await db
          .collection("productImages.files")
          .findOne({ "metadata.sourcePath": publicPath });

        const fileId = await uploadImage(bucket, db, publicPath);

        if (!previouslyUploaded) {
          uploadedImages++;
        }

        imageRefs.push({
          sourcePath: publicPath,
          fileId,
          url: `/api/images/${fileId}`,
        });
      } catch (error) {
        missingImages++;
        console.warn(
          `Missing/unreadable image for ${product.type}#${product.id}: ${publicPath}`,
        );
      }
    }

    const mainImage =
      imageRefs.find((ref) => ref.sourcePath === product.image) ??
      imageRefs[0];

    const document = {
      ...product,

      // Stable ID prevents duplicate product records on reruns.
      _id: `${product.type}:${product.id}`,
      productId: product.id,
      type: product.type,

      image: mainImage?.url ?? null,
      images: imageRefs.map((ref) => ref.url),
      imageRefs,

      updatedAt: new Date(),
    };

    await productsCollection.replaceOne(
      { _id: document._id },
      document,
      { upsert: true },
    );

    importedProducts++;

    console.log(
      `Imported ${product.type} #${product.id}: ${product.name} (${imageRefs.length} images)`,
    );
  }

  await productsCollection.createIndex(
    { type: 1, productId: 1 },
    { unique: true },
  );

  console.log("\n========== IMPORT SUMMARY ==========");
  console.log(`Products processed: ${importedProducts}`);
  console.log(`New images uploaded: ${uploadedImages}`);
  console.log(`Missing/unreadable image references: ${missingImages}`);
  console.log("MongoDB import finished.");

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error("Import failed:", error);
  await mongoose.disconnect().catch(() => undefined);
  process.exitCode = 1;
});
