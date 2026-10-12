-- Esquema de CADA shard horizontal (misma estructura, filas distintas).
-- Fragmento "core" del cliente (sharding VERTICAL: columnas calientes/livianas).
CREATE TABLE IF NOT EXISTS clientes_core (
    id         UUID PRIMARY KEY,
    nombre     TEXT        NOT NULL,
    email      TEXT        NOT NULL UNIQUE,
    ciudad     TEXT        NOT NULL,
    creado_en  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_clientes_ciudad ON clientes_core (ciudad);

-- Pedidos co-ubicados con su cliente (misma shard key = cliente_id).
CREATE TABLE IF NOT EXISTS pedidos (
    id         UUID PRIMARY KEY,
    cliente_id UUID           NOT NULL REFERENCES clientes_core(id) ON DELETE CASCADE,
    total      NUMERIC(12, 2) NOT NULL,
    creado_en  TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_pedidos_cliente ON pedidos (cliente_id);

CREATE TABLE IF NOT EXISTS pedido_items (
    pedido_id   UUID    NOT NULL REFERENCES pedidos(id) ON DELETE CASCADE,
    producto_id INT     NOT NULL,
    cantidad    INT     NOT NULL CHECK (cantidad > 0),
    precio      NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (pedido_id, producto_id)
);

-- Tabla de referencia REPLICADA en todos los shards (catálogo pequeño y de solo lectura):
-- evita joins entre shards.
CREATE TABLE IF NOT EXISTS productos (
    id     INT PRIMARY KEY,
    nombre TEXT           NOT NULL,
    precio NUMERIC(12, 2) NOT NULL
);
INSERT INTO productos (id, nombre, precio) VALUES
    (1, 'Teclado mecánico',   120000),
    (2, 'Mouse inalámbrico',   60000),
    (3, 'Monitor 24"',        650000),
    (4, 'Auriculares',        180000),
    (5, 'Webcam HD',          140000),
    (6, 'Disco SSD 1TB',      320000),
    (7, 'Memoria RAM 16GB',   210000),
    (8, 'Router WiFi 6',      250000)
ON CONFLICT (id) DO NOTHING;
