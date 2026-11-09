CREATE TABLE IF NOT EXISTS Taxis (
  id INT PRIMARY KEY,
  availability VARCHAR(10),
  movement VARCHAR(10),
  localizacion_x INT,
  localizacion_y INT
);

CREATE TABLE IF NOT EXISTS Clientes (
  id INT PRIMARY KEY,
  localizacion_x INT,
  localizacion_y INT,
  recogido BOOLEAN,
  destination_x INT,
  destination_y INT
);

CREATE TABLE IF NOT EXISTS Locations (
  id VARCHAR(10) PRIMARY KEY,
  pos_x INT,
  pos_y INT
);

INSERT IGNORE INTO Clientes (
  id,
  localizacion_x,
  localizacion_y,
  recogido,
  destination_x,
  destination_y
) VALUES
  (1, 7, 3, false, NULL, NULL),
  (2, 9, 14, false, NULL, NULL),
  (3, 19, 10, false, NULL, NULL),
  (4, 4, 15, false, NULL, NULL),
  (5, 16, 5, false, NULL, NULL);

INSERT IGNORE INTO Locations (id, pos_x, pos_y) VALUES
  ('A', 5, 3),
  ('B', 2, 11),
  ('C', 8, 16),
  ('D', 17, 16),
  ('E', 19, 10),
  ('F', 11, 5);
