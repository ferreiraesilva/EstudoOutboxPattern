CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ERP Table 1: pessoas
CREATE TABLE IF NOT EXISTS pessoas (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    nome VARCHAR(255) NOT NULL,
    cpf VARCHAR(14) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ERP Table 2: emails
CREATE TABLE IF NOT EXISTS emails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pessoa_id UUID NOT NULL REFERENCES pessoas(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ERP Table 3: enderecos
CREATE TABLE IF NOT EXISTS enderecos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pessoa_id UUID NOT NULL REFERENCES pessoas(id) ON DELETE CASCADE,
    logradouro VARCHAR(255) NOT NULL,
    cidade VARCHAR(100) NOT NULL,
    uf VARCHAR(2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ERP Table 4: telefones
CREATE TABLE IF NOT EXISTS telefones (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pessoa_id UUID NOT NULL REFERENCES pessoas(id) ON DELETE CASCADE,
    ddd VARCHAR(3) NOT NULL,
    numero VARCHAR(10) NOT NULL,
    tipo VARCHAR(20) DEFAULT 'CELULAR',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- APP Outbox Table: outbox_events
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE pessoas REPLICA IDENTITY FULL;
ALTER TABLE emails REPLICA IDENTITY FULL;
ALTER TABLE enderecos REPLICA IDENTITY FULL;
ALTER TABLE telefones REPLICA IDENTITY FULL;
ALTER TABLE outbox_events REPLICA IDENTITY FULL;
