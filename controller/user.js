const express = require("express");
const router = express.Router();
const { pool } = require('../db');
const bcrypt = require('bcrypt');
 
// ================
// Role APIs
// ================
 
// Add role
const addRole = async (req, res) => {
    try {
        const { name, description, permissions } = req.body;
        const query = `
            INSERT INTO sm_roles (name, description, permissions)
            VALUES ($1, $2, $3::jsonb) RETURNING *`;
        const { rows } = await pool.query(query, [name, description, JSON.stringify(permissions)]);
        res.json(rows[0]);
    } catch (err) {
        console.error("Error adding role:", err);
        res.status(500).json({ error: err.message });
    }
};
 
// Fetch roles
const fetchRole = async (req, res) => {
    try {
        const query = `
            SELECT r.id, r.name, r.description, r.permissions,
                   COUNT(u.id) AS user_count
            FROM sm_roles r
            LEFT JOIN sm_users u ON u.role_id = r.id
            GROUP BY r.id, r.name, r.description, r.permissions
            ORDER BY r.id ASC
        `;
        const { rows } = await pool.query(query);
        res.json(rows);
    } catch (err) {
        console.error("Error fetching roles:", err);
        res.status(500).json({ error: err.message });
    }
};
 
// Update role
const updateRole = async (req, res) => {
    try {
        const { id } = req.params;
        const { name, description, permissions } = req.body;
        const query = `
            UPDATE sm_roles
            SET name=$1, description=$2, permissions=$3::jsonb
            WHERE id=$4 RETURNING *`;
        const { rows } = await pool.query(query, [name, description, JSON.stringify(permissions), id]);
        res.json(rows[0]);
    } catch (err) {
        console.error("Error updating role:", err);
        res.status(500).json({ error: err.message });
    }
};
 
// Delete role
const deleteRole = async (req, res) => {
    try {
        const { id } = req.params;
        await pool.query('DELETE FROM sm_roles WHERE id=$1', [id]);
        res.json({ message: 'Role deleted successfully' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
};
 
 
// ================
// User APIs
// ================
 
// Add User
const addUser = async (req, res) => {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
 
        const { full_name, user_name, email, designation, role_id, status, password, is_admin, parameters } = req.body;
 
        const hashedPassword = await bcrypt.hash(password, 10);
 
        const insertUserQuery = `
          INSERT INTO sm_users (full_name, user_name, email, designation, role_id, status, password, is_admin, parameters, created_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, NOW())
          RETURNING id,status;
        `;
 
        const { rows } = await client.query(insertUserQuery, [
            full_name,
            user_name,
            email,
            designation,
            role_id,
            status,
            hashedPassword,
            is_admin || false,
            JSON.stringify(parameters || {})
        ]);
 
        const newUser = rows[0];
       if (newUser.status === true || newUser.status === 1) {
            const insertLogQuery = `
              INSERT INTO sm_status_logs (user_id, activated_at)
              VALUES ($1, NOW());
            `;
            await client.query(insertLogQuery, [newUser.id]);
        }
 
        await client.query("COMMIT");
 
        res.json(newUser);
    } catch (err) {
        await client.query("ROLLBACK");
        console.error("Error adding user:", err);
        res.status(500).json({ error: err.message });
    } finally {
        client.release();
    }
};
 
 
 
// Fetch all users (with role name)
const fetchUsers = async (req, res) => {
    try {
        const query = `
      SELECT u.id, u.full_name, u.user_name, u.email, u.designation,
             u.status, u.is_admin, u.parameters, u.created_at,
             r.id AS role_id, r.name AS role_name
      FROM sm_users u
      LEFT JOIN sm_roles r ON u.role_id = r.id
      ORDER BY u.id ASC;
    `;
        const { rows } = await pool.query(query);
        res.json(rows);
    } catch (err) {
        console.error("Error fetching users:", err);
        res.status(500).json({ error: err.message });
    }
};
 
// Update User
const updateUser = async (req, res) => {
    try {
        console.log("Incoming data:", req.body);

        const { id } = req.params;
        const { full_name, user_name, email, designation, role_id, status, password, parameters } = req.body;
        // 🔹 Step 1: Get current status
        const currentUserResult = await pool.query(
            `SELECT status FROM sm_users WHERE id=$1`,
            [id]
        );

        if (currentUserResult.rows.length === 0) {
            return res.status(404).json({ error: "User not found" });
        }

        const currentStatus = currentUserResult.rows[0].status;

        let query;
        let values;

        if (password) {
            const hashedPassword = await bcrypt.hash(password, 10);
            query = `
        UPDATE sm_users
        SET full_name=$1, user_name=$2, email=$3, designation=$4, role_id=$5,
            status=$6, password=$7, parameters=$8
        WHERE id=$9 RETURNING *;
      `;
            values = [full_name, user_name, email, designation, role_id, status, hashedPassword, parameters, id];
        } else {
            query = `
        UPDATE sm_users
        SET full_name=$1, user_name=$2, email=$3, designation=$4, role_id=$5,
            status=$6, parameters=$7
        WHERE id=$8 RETURNING *;
      `;
            values = [full_name, user_name, email, designation, role_id, status, parameters, id];
        }

        const { rows } = await pool.query(query, values);
        const updatedUser = rows[0];

        if (!updatedUser) {
            return res.status(404).json({ error: "User not found" });
        }
        // Convert both to numbers (if DB is int) or booleans (if DB is bool)
        const newStatus = (status === true || status === "1" || status === 1) ? 1 : 0;
        const dbStatus = (currentStatus === true || currentStatus === "1" || currentStatus === 1) ? 1 : 0;

        // 🔹 Step 2: If status changed, update sm_status_logs
        if (newStatus !== dbStatus) {
            if (newStatus === 1) {
                console.log("Inserting activation log for user", id);
                await pool.query(
                    `INSERT INTO sm_status_logs (user_id, activated_at) VALUES ($1, NOW())`,
                    [id]
                );
            } else {
                console.log("Deactivating latest log for user", id);
                await pool.query(
                    `UPDATE sm_status_logs
       SET inactivated_at = NOW()
       WHERE id = (
         SELECT id
         FROM sm_status_logs
         WHERE user_id = $1 AND inactivated_at IS NULL
         ORDER BY activated_at DESC
         LIMIT 1
       )`,
                    [id]
                );
            }
        }
        res.json(updatedUser);

    } catch (err) {
        console.error("Error updating user:", err);
        res.status(500).json({ error: err.message });
    }
};
 
// Delete User
const deleteUser = async (req, res) => {
    try {
        const { id } = req.params;
        await pool.query(`DELETE FROM sm_users WHERE id=$1`, [id]);
        res.json({ message: "User deleted successfully" });
    } catch (err) {
        console.error("Error deleting user:", err);
        res.status(500).json({ error: err.message });
    }
};
 
const getAllusers = async (req, res) => {
    try {
        const result = await pool.query(`
      SELECT
          u.id AS user_id,
          u.full_name,
          u.user_name,
          u.email,
          u.designation,
          u.status,
          u.failed_attempts,
          u.on_hold_time,
          u.is_admin,
          u.created_at,
          u.parameters,
 
          r.name AS role_name,
          r.description AS role_description,
          r.permissions AS role_permissions,
 
          s.login_time,
          s.logout_time,
          s.active_hours,
 
          st.activated_at AS latest_status,
          st.inactivated_at AS status_changed_at,
          st.active_duration AS duration
 
      FROM sm_users u
      LEFT JOIN sm_roles r
          ON u.role_id = r.id
      LEFT JOIN sm_session_logs s
          ON s.user_id = u.id
          AND s.login_time = (
              SELECT MAX(s2.login_time)
              FROM sm_session_logs s2
              WHERE s2.user_id = u.id
          )
      LEFT JOIN sm_status_logs st
          ON st.user_id = u.id
          AND st.activated_at = (
              SELECT MAX(st2.activated_at)
              FROM sm_status_logs st2
              WHERE st2.user_id = u.id
          )
      ORDER BY u.created_at DESC;
    `);
 
        res.json(result.rows);
    } catch (err) {
        console.error("Error fetching users:", err);
        res.status(500).send(err.message);
    }
};
 
 
const getUserDetails = async (req, res) => {
    const { id } = req.params;
 
    try {
        const result = await pool.query(`
      SELECT
          u.id,
          u.full_name,
          u.user_name,
          u.email,
          u.designation,
          u.status,
          u.failed_attempts,
          u.on_hold_time,
          u.is_admin,
          u.created_at,
          u.parameters,
 
          r.name AS role_name,
          r.description AS role_description,
          r.permissions AS role_permissions,
 
          s.login_time,
          s.logout_time,
          s.active_hours,
 
          st.activated_at AS latest_status,
          st.inactivated_at AS status_changed_at,
          st.active_duration AS duration
 
      FROM sm_users u
      LEFT JOIN sm_roles r
          ON u.role_id = r.id
      LEFT JOIN sm_session_logs s
          ON s.user_id = u.id
          AND s.login_time = (
              SELECT MAX(s2.login_time)
              FROM sm_session_logs s2
              WHERE s2.user_id = u.id
          )
      LEFT JOIN sm_status_logs st
          ON st.user_id = u.id
          AND st.activated_at = (
              SELECT MAX(st2.activated_at)
              FROM sm_status_logs st2
              WHERE st2.user_id = u.id
          )
      WHERE u.id = $1
      ORDER BY u.created_at DESC
      LIMIT 1;
    `, [id]);
 
        if (result.rows.length === 0) {
            return res.status(404).json({ message: "User not found" });
        }
 
        res.json(result.rows[0]);
    } catch (err) {
        console.error("Error fetching user details:", err);
        res.status(500).send(err.message);
    }
};
 
const updateUserStatus = async (req, res) => {
    try {
        const { id } = req.params;
        const { status } = req.body;  // true | false
      const userId = parseInt(id, 10);
    if (isNaN(userId)) {
      return res.status(400).json({ error: "Invalid user id" });
    }
        // Step 1: Update sm_users
        const updateUserQuery = `
      UPDATE sm_users
      SET status = $1
      WHERE id = $2
      RETURNING id, status;
    `;
        const { rows } = await pool.query(updateUserQuery, [status, userId]);
 
        if (rows.length === 0) {
            return res.status(404).json({ error: "User not found" });
        }
 
        // Step 2: Insert/update sm_status_logs
        let logQuery, logValues;
        if (status) {
            logQuery = `
        INSERT INTO sm_status_logs (user_id, activated_at)
        VALUES ($1, NOW())
        RETURNING *;
      `;
            logValues = [userId];
        } else {
            logQuery = `
        UPDATE sm_status_logs
        SET inactivated_at = NOW()
        WHERE user_id = $1
          AND inactivated_at IS NULL
        RETURNING *;
      `;
            logValues = [userId];
        }
 
        const logResult = await pool.query(logQuery, logValues);
 
        res.json({
            user: rows[0],
            log: logResult.rows[0] || null
        });
 
    } catch (err) {
        console.error("Error updating user status:", err);
        res.status(500).json({ error: err.message });
    }
};
 
// ===============
// Add / Edit User
//================
 
//fetch roles for add user
const getroles = async (req, res) => {
    try {
        const result = await pool.query("SELECT * FROM sm_roles");
        res.json(result.rows);
    } catch (err) {
        res.status(500).send(err.message);
    }
};
 
//Check existing user
const checkusername = async (req, res) => {
    const { user_name, email } = req.body
    try {
        const result = await pool.query(
            'SELECT COUNT(*) AS count FROM sm_users WHERE user_name = $1',
            [user_name]
        );
        const email_result = await pool.query(
            'SELECT COUNT(*) AS count FROM sm_users WHERE email = $1',
            [email]
        );
        res.json({
            usernameExists: result.rows[0].count > 0,
            emailExists: email_result.rows[0].count > 0
        });
    } catch (error) {
        console.error('Error in checkUsername:', error);
        res.status(500).json({ message: 'Error checking username.' });
    }
};
 
//get total count
const getCounts = async (req, res) => {
    try {
        const result = await pool.query(`
      SELECT
        (SELECT COUNT(*) FROM sm_users) AS total_users,
        (SELECT COUNT(*) FROM sm_roles) AS total_roles,
        (SELECT COUNT(*) FROM sm_users WHERE status = 'true') AS active_users,
        (SELECT COUNT(*) FROM sm_users WHERE status = 'false') AS inactive_users
    `);
 
        res.json(result.rows[0]); // returns an object with all 4 counts
    } catch (err) {
        console.error("Error fetching counts:", err);
        res.status(500).send(err.message);
    }
};
 
// Get User By ID
const getUserById = async (req, res) => {
    const { id } = req.params; // fetch id from URL params
 
    try {
        const result = await pool.query(
            `SELECT
         u.id,
         u.full_name,
         u.user_name,
         u.email,
         u.designation,
         u.status,
         u.role_id,
         u.parameters,
         u.created_at,
         r.name as role_name
       FROM sm_users u
       LEFT JOIN sm_roles r ON u.role_id = r.id
       WHERE u.id = $1`,
            [id]
        );
 
        if (result.rows.length === 0) {
            return res.status(404).json({ message: "User not found" });
        }
 
        res.json(result.rows[0]);
    } catch (error) {
        console.error("Error fetching user:", error);
        res.status(500).json({ error: error.message });
    }
};
 
// ===============
// Login / Logout
// ===============
 
const logoutUser = async (req, res) => {
    const { sessionId } = req.body;  // frontend will send this
    const logoutTime = new Date();

    try {
        // Update session log
        await pool.query(
            `UPDATE sm_session_logs
       SET logout_time = $1
       WHERE id = $2`,
            [logoutTime, sessionId]
        );

        res.status(200).json({ message: 'Logout successful' });
    } catch (error) {
        console.error("Logout error:", error);
        res.status(500).json({ error: error.message });
    }
};

const loginUser = async (req, res) => {
    const { user_name, password } = req.body;
    console.log("Attempting login for:", user_name);

    try {
        const result = await pool.query('SELECT * FROM sm_users WHERE user_name = $1', [user_name]);

        if (result.rows.length === 0) {
            console.log("No user found");
            return res.status(401).json({ message: 'User not found' });
        }

        const user = result.rows[0];

        console.log("Found user:", user.user_name);
        // ✅ Check if user is inactive
        if (user.status === false) {
            return res.status(403).json({ message: "User is not active. Please contact admin." });
        }

        // ✅ Check if user is locked
        if (user.on_hold_time && new Date(user.on_hold_time) > new Date()) {
            const minutesLeft = Math.ceil((new Date(user.on_hold_time) - new Date()) / 60000);
            return res.status(403).json({
                message: `Account locked. Try again after ${minutesLeft} minutes.`
            });
        }
        // const hashedPassword = await bcrypt.hash(password, 10);

        // ✅ Compare password
        const isMatch = await bcrypt.compare(password, user.password);
        if (!isMatch) {
            let failedAttempts = user.failed_attempts + 1;

            if (failedAttempts >= 3) {
                // Lock for 1 hour
                const lockUntil = new Date(Date.now() + 60 * 60 * 1000); // 1 hour
                await pool.query(
                    'UPDATE sm_users SET failed_attempts = $1, on_hold_time = $2 WHERE user_name = $3',
                    [failedAttempts, lockUntil, user_name]
                );
                return res.status(403).json({ message: 'Account locked due to multiple failed attempts. Try again after 1 hour.' });
            } else {
                // Just increment failed attempts
                await pool.query(
                    'UPDATE sm_users SET failed_attempts = $1 WHERE user_name = $2',
                    [failedAttempts, user_name]
                );
                return res.status(401).json({ message: 'Invalid password' });
            }
        }

        // ✅ fetch role permissions
        const roleRes = await pool.query(
            'SELECT * FROM sm_roles WHERE id = $1',
            [user.role_id]
        );
        const role = roleRes.rows[0];
        let permissions = role.permissions;
        if (typeof permissions === 'string') {
            try {
                permissions = JSON.parse(permissions);
            } catch (e) {
                console.error("Error parsing permissions:", e);
                permissions = {};
            }
        }


        // ✅ On successful login → reset failed attempts
        await pool.query(
            'UPDATE sm_users SET failed_attempts = 0, on_hold_time = NULL WHERE user_name = $1',
            [user_name]
        );

        // ✅ Insert session log entry
        const loginTime = new Date();
        const session = await pool.query(
            `INSERT INTO sm_session_logs (user_id, login_time)
   VALUES ($1, $2) RETURNING id`,
            [user.id, loginTime]
        );

        console.log("Login successful, session created:", session.rows[0].id);
        res.status(200).json({
            message: 'Login successful',
            user: {
                id: user.id,
                full_name: user.full_name,
                user_name: user.user_name,
                role: role.name,
                permissions,
            },
            sessionId: session.rows[0].id
        });

    } catch (error) {
        console.error("Login error:", error);
        res.status(500).json({ error: error.message });
    }
};
 
//Check requested user verify
const forget_password_request = async (req, res) => {
    const { user_name, email_id } = req.body;
 
    try {
        // Check if username exists
        const userResult = await pool.query(
            'SELECT * FROM sm_users WHERE user_name = $1',
            [user_name]
        );
 
        if (userResult.rows.length === 0) {
            return res.json({ valid: false, reason: 'username' });
        }
 
        // Check if email matches the found user
        const emailMatch = userResult.rows.find(u => u.email === email_id);
        if (!emailMatch) {
            return res.json({ valid: false, reason: 'email' });
        }
        res.json({ valid: true });
    } catch (err) {
        console.error('Forget password request error:', err);
        res.status(500).json({ message: 'Server error' });
    }
};
 
//check the username, password and email exists
const forget_password = async (req, res) => {
    const { user_name, email_id, password } = req.body;
 
    try {
        // 1. Check if username exists
        const userResult = await pool.query(
            "SELECT * FROM sm_users WHERE user_name = $1",
            [user_name]
        );
        console.log("data", userResult)
        if (userResult.rows.length === 0) {
            return res.json({ valid: false, reason: "username" });
        }
 
        const user = userResult.rows[0];
 
        // 2. Check if email matches
        if (user.email !== email_id) {
            return res.json({ valid: false, reason: "email" });
        }
 
        // 3. Compare password using bcrypt
        const isMatch = await bcrypt.compare(password, user.password);
        if (!isMatch) {
            return res.json({ valid: false, reason: "password" });
        }
 
        // 4. All good
        return res.json({ valid: true });
 
    } catch (err) {
        console.error("Forget password error:", err);
        res.status(500).json({ message: "Server error" });
    }
};
 
//change the password
const change_password = async (req, res) => {
    const { user_name, newPassword } = req.body;
    const encrypt_password = await bcrypt.hash(newPassword, 10);
 
    try {
        await pool.query(
            'UPDATE sm_users SET password = $1 WHERE user_name = $2',
            [encrypt_password, user_name]
        );
        res.json({ success: true });
    } catch (err) {
        res.status(500).send(err.message);
    }
}
 
 
//loged in user count
const getActiveUsers = async (req, res) => {
  try {
    const query = `
      SELECT
        s.id AS session_id,
        s.user_id,
        s.login_time,
        u.user_name,
        u.full_name
      FROM sm_session_logs s
      JOIN sm_users u ON s.user_id = u.id
      WHERE s.logout_time IS NULL
      ORDER BY s.login_time DESC;
    `;
 
    const { rows } = await pool.query(query);
 
    res.status(200).json({
      count: rows.length,
      users: rows
    });
  } catch (error) {
    console.error("Error fetching active users:", error);
    res.status(500).json({ error: error.message });
  }
};
 
 
// ===============
// Export
// ===============
module.exports = {
 
    addRole,
    fetchRole,
    updateRole,
    deleteRole,
    addUser,
    fetchUsers,
    // fetchUserById,
    updateUser,
    deleteUser,
    getAllusers,
    getUserDetails,
    updateUserStatus,
    getroles,
    checkusername,
    getCounts,
    logoutUser,
    loginUser,
    getUserById,
    forget_password_request,
    forget_password,
    change_password,
    getActiveUsers
 
};