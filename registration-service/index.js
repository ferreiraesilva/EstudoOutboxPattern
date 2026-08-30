import express from 'express';
import pkg from 'pg';

const { Pool } = pkg;
const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://appuser:apppass@postgres:5432/appdb'
});

app.post('/pessoas', async (req, res) => {
  const { nome, cpf, email, logradouro, cidade, uf, ddd, numero } = req.body;

  if (!nome || !cpf || !email || !logradouro || !cidade || !uf || !ddd || !numero) {
    return res.status(400).json({
      error: 'Campos obrigatórios: nome, cpf, email, logradouro, cidade, uf, ddd, numero'
    });
  }

  const client = await pool.connect();

  try {
    // Transação isolada do ERP em 4 tabelas
    await client.query('BEGIN');

    // 1. Tabela pessoas
    const pessoaRes = await client.query(
      `INSERT INTO pessoas (nome, cpf) VALUES ($1, $2) RETURNING id, nome, cpf, created_at;`,
      [nome, cpf]
    );
    const pessoa = pessoaRes.rows[0];

    // 2. Tabela emails
    const emailRes = await client.query(
      `INSERT INTO emails (pessoa_id, email) VALUES ($1, $2) RETURNING id, email;`,
      [pessoa.id, email]
    );
    const emailData = emailRes.rows[0];

    // 3. Tabela enderecos
    const enderecoRes = await client.query(
      `INSERT INTO enderecos (pessoa_id, logradouro, cidade, uf) VALUES ($1, $2, $3, $4) RETURNING id, logradouro, cidade, uf;`,
      [pessoa.id, logradouro, cidade, uf]
    );
    const enderecoData = enderecoRes.rows[0];

    // 4. Tabela telefones
    const telefoneRes = await client.query(
      `INSERT INTO telefones (pessoa_id, ddd, numero) VALUES ($1, $2, $3) RETURNING id, ddd, numero;`,
      [pessoa.id, ddd, numero]
    );
    const telefoneData = telefoneRes.rows[0];

    await client.query('COMMIT');

    console.log(`[ERP Service] Cadastro concluído no banco do ERP em 4 tabelas para pessoa ID: ${pessoa.id}`);

    res.status(201).json({
      message: 'Pessoa cadastrada com sucesso no ERP (4 tabelas inseridas)',
      pessoa: {
        ...pessoa,
        email: emailData,
        endereco: enderecoData,
        telefone: telefoneData
      }
    });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('[ERP Service] Erro na transação do ERP, rollback executado:', error.message);
    res.status(500).json({ error: 'Erro ao cadastrar pessoa no ERP', details: error.message });
  } finally {
    client.release();
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[ERP Service] Rodando na porta ${PORT}`);
});
