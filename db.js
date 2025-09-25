const { Pool } = require("pg");
const pool = new Pool({
  user: "tridel",
  password: "3Del@sanmar@2025",
  host: "trideltechnologiesindia.com",
  port: 5432,
  database: "db_sanmar",
});
const connectDB = async () => {
  try {
    const client = await pool.connect();
    console.log("Connected to PostgreSQL");
    client.release();
  } catch (err) {
    console.error("Database connection failed:", err);
  }
};
module.exports = { pool, connectDB };
