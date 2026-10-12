-- Nodo VERTICAL: columnas "frías"/pesadas del cliente (bio, avatar, preferencias).
-- Se separan de clientes_core para que las lecturas frecuentes no arrastren datos grandes.
CREATE TABLE IF NOT EXISTS clientes_perfil (
    cliente_id   UUID PRIMARY KEY,          -- misma clave que clientes_core.id (sin FK: otro servidor)
    bio          TEXT,
    avatar       BYTEA,
    preferencias JSONB NOT NULL DEFAULT '{}'::jsonb,
    actualizado  TIMESTAMPTZ NOT NULL DEFAULT now()
);
