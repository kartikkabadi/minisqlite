DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  department TEXT,
  salary REAL,
  active INTEGER DEFAULT 1,
  manager_id INTEGER
);
INSERT INTO employees (name, department, salary, active, manager_id) VALUES
  ('Alice', 'Engineering', 100000, 1, NULL),
  ('Bob', 'Engineering', 120000, 1, 1),
  ('Charlie', 'Sales', 80000, 1, 1),
  ('David', 'Sales', 95000, 0, 1),
  ('Eve', 'HR', 75000, 1, NULL);
SELECT name, salary FROM employees WHERE salary > 100000 ORDER BY salary DESC;
SELECT department, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY department HAVING cnt > 1;
SELECT name, salary FROM employees WHERE department IN ('Engineering', 'Sales');
SELECT * FROM employees WHERE name LIKE '%li%';
SELECT * FROM employees WHERE salary BETWEEN 90000 AND 120000;
SELECT name, CASE WHEN salary > 100000 THEN 'high' ELSE 'normal' END AS tier FROM employees;
SELECT UPPER(name), ROUND(salary / 12, 2) AS monthly FROM employees LIMIT 3;
BEGIN;
UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering';
DELETE FROM employees WHERE active = 0;
COMMIT;
PRAGMA table_info(employees);
ALTER TABLE employees ADD COLUMN email TEXT DEFAULT 'unknown@example.com';
.tables
.schema
.stats
.quit
