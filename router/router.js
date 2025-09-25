const express = require("express");
const {
  sendMail,
  fetchLive,
  fetchAverages,
  fetchTideObs,
  fetchAllData,
  fetchAverageData,
  fetchDataHealthReport,
  fetchDataHealthChart,
  fetchWindData,
  fetchCurrentData,
  insertLogs,
  fetchReportData,
  fetchLive2,
} = require("../controller/controller");
const router = express.Router();
const userController = require("../controller/user");

// Role routes
router.post("/addRole", userController.addRole);
router.get("/fetchRole", userController.fetchRole);
router.put("/updateRole/:id", userController.updateRole);
router.delete("/deleteRole/:id", userController.deleteRole);

// User Routes
router.post("/users", userController.addUser);
router.get("/users", userController.fetchUsers);

// router.get("/users/:id", userController.fetchUserById);
router.put("/users/:id", userController.updateUser);
router.delete("/users/:id", userController.deleteUser);
router.get("/users/all", userController.getAllusers);
router.get("/users/:id/details", userController.getUserDetails);
router.put("/users/:id/status", userController.updateUserStatus);
router.get("/getroles", userController.getroles);
router.post("/check", userController.checkusername);
router.get("/counts", userController.getCounts);
router.get("/users/:id", userController.getUserById);
router.get("/currect_login", userController.getActiveUsers);

//Login, Logout Routes
router.post("/logout", userController.logoutUser);
router.post("/login", userController.loginUser);
router.post("/verifyUser", userController.forget_password);
router.post("/resetPassword", userController.change_password);
router.post("/forgotrequest", userController.forget_password_request);

// Notification Routes
router.post("/sendmail", sendMail);
router.get("/get_dash_data", fetchLive);
router.get("/get_dash_data2", fetchLive2);
router.get("/averages", fetchAverages);

router.get("/fetchTideObs", fetchTideObs);
router.get("/fetchAllData", fetchAllData);
router.get("/fetchAverageData", fetchAverageData);
router.get("/fetchDataHealthReport", fetchDataHealthReport);
router.get("/fetchDataHealthChart", fetchDataHealthChart);
router.get("/fetchWindData", fetchWindData);
router.get("/fetchCurrentData", fetchCurrentData);
router.post("/insertLogs", insertLogs);
router.get("/fetchReportData", fetchReportData);

module.exports = router;
