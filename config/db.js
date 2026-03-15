import mongoose from "mongoose";

let _db = null;

export const connectDatabases = async () => {
  const uri = process.env.MONGO_URI;

  if (!uri) {
    console.error("❌ DB Error: MONGO_URI missing in .env");
    process.exit(1);
  }

  try {
    await mongoose.connect(uri);
    _db = mongoose.connection;
    console.log(`✅ MongoDB Connected: ${_db.name}`);
  } catch (err) {
    console.error("❌ DB Connection Error:", err.message);
    process.exit(1);
  }

  mongoose.connection.on("disconnected", () =>
    console.warn("⚠️  MongoDB disconnected!")
  );
  mongoose.connection.on("error", (err) =>
    console.error("❌ MongoDB error:", err.message)
  );
};

export const getDebitNeutralDB  = () => mongoose.connection;
export const getTrafficDB = () => mongoose.connection;