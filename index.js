const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const { connectDB, pool } = require("./db");
const userRouter = require("./router/router");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(bodyParser.json({ limit: "50mb" }));
app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);
app.use(express.json({ limit: "50mb" }));

// Increase URL-encoded payload limit
app.use(express.urlencoded({ limit: "52428800", extended: true }));
// app.use(express.limit(100000000));
connectDB();

app.use("/api", userRouter);

app.listen(PORT, () => {
  console.log(`Server is running on http://192.168.0.7:${PORT}`);
});
