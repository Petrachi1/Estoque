// server.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import fs from 'fs/promises';
import path from 'path';
import XLSX from 'xlsx';

const app = express();
app.use(cors());
app.use(express.json());

// ---------- Utils ----------
async function getDropboxAccessToken() {
  const url = 'https://api.dropboxapi.com/oauth2/token';
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: process.env.DROPBOX_REFRESH_TOKEN,
    client_id: process.env.DROPBOX_APP_KEY,
    client_secret: process.env.DROPBOX_APP_SECRET,
  });

  const res = await fetch(url, { method: 'POST', body });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Dropbox token error ${res.status}: ${txt}`);
  }
  const json = await res.json();
  return json.access_token;
}

async function dropboxDownloadFile(dbxPath) {
  const access = await getDropboxAccessToken();
  const url = 'https://content.dropboxapi.com/2/files/download';
  const headers = {
    Authorization: `Bearer ${access}`,
    'Dropbox-API-Arg': JSON.stringify({ path: dbxPath }),
  };
  const res = await fetch(url, { method: 'POST', headers });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Dropbox download ${res.status}: ${txt}`);
  }
  const arrayBuffer = await res.arrayBuffer();
  return Buffer.from(arrayBuffer);
}

/**
 * Lê planilha e retorna linhas em JSON.
 * - sheetName: aba a ler
 * - headerRow: número da linha de cabeçalho (1-based)
 */
function workbookToRows(buffer, sheetName, headerRow = 1) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[sheetName || wb.SheetNames[0]];
  if (!ws) throw new Error(`Aba não encontrada: ${sheetName}`);

  // Ajusta range para começar no headerRow
  const range = XLSX.utils.decode_range(ws['!ref']);
  const startRowIdx = Math.max(0, (Number(headerRow) || 1) - 1);
  range.s.r = startRowIdx;
  ws['!ref'] = XLSX.utils.encode_range(range);

  // sheet_to_json vai usar a linha de header como cabeçalho das chaves
  const rows = XLSX.utils.sheet_to_json(ws, { defval: '' });
  return rows;
}

// Normalização leve para manter compatível com o front
function mapFields(row) {
  const get = (keys, def = '') => {
    for (const k of keys) if (k in row && row[k] !== '') return String(row[k]).trim();
    return def;
  };
  return {
    // Front trata: nome, qtd, und, ia, pragas, registro, empresa, formulacao
    nome: get(['nome_comercial','Nome Comercial','nome','Produto','produto']),
    qtd: Number(get(['quantidade','qtd','estoque','saldo'], '0').replace(',', '.')) || 0,
    und: get(['unidade','un','und','u.m.']),
    ia: get(['ia','ingrediente_ativo','ingrediente ativo','IAs']),
    pragas: get(['pragas','praga','alvos','pragas_alvo']),
    registro: get(['registro','numero_registro','n_registro','Nº Registro']),
    empresa: get(['empresa','titular_registro','registrante']),
    formulacao: get(['formulacao','formulação','form']),
  };
}

// ---------- Endpoints ----------
app.get('/api/estoque', async (req, res) => {
  try {
    const filePath = process.env.DROPBOX_FILE_PATH;
    if (!filePath) return res.status(500).json({ error: 'DROPBOX_FILE_PATH não configurado.' });

    const buf = await dropboxDownloadFile(filePath);
    const rows = workbookToRows(buf, process.env.EXCEL_SHEET, process.env.EXCEL_HEADER_ROW);
    // Normaliza para o front (mas mantenho as colunas originais também)
    const normalized = rows
      .map(r => ({ ...r, __mapped: mapFields(r) }))
      .filter(r => r.__mapped.nome);

    // Você pode escolher devolver já mapeado ou bruto.
    // Aqui devolvo a versão mapeada que o front entende bem:
    const mapped = normalized.map(r => r.__mapped);

    res.set('Cache-Control', 'no-store');
    return res.json(mapped);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e.message || e) });
  }
});

app.get('/api/catalogo', async (req, res) => {
  try {
    const dropboxCat = process.env.CATALOGO_DROPBOX_PATH;
    let rows = [];

    if (dropboxCat) {
      // Catálogo também no Dropbox (recomendado p/ Render)
      const buf = await dropboxDownloadFile(dropboxCat);
      rows = workbookToRows(buf, null, 1); // considera header na linha 1 do catálogo
    } else {
      // DEV: caminho local
      const localPath = process.env.CATALOGO_PATH;
      if (!localPath) throw new Error('Defina CATALOGO_PATH ou CATALOGO_DROPBOX_PATH');

      const ext = path.extname(localPath).toLowerCase();
      if (ext === '.json') {
        const txt = await fs.readFile(localPath, 'utf-8');
        rows = JSON.parse(txt);
      } else {
        const buf = await fs.readFile(localPath);
        rows = workbookToRows(buf, null, 1);
      }
    }

    // Normaliza chaves principais do catálogo p/ front
    const mapped = rows.map((r) => ({
      nome: r.nome_comercial || r.nome || r.Produto || r.produto || r['Nome Comercial'] || '',
      ia: r.ia || r.ingrediente_ativo || r['ingrediente ativo'] || r.IAs || '',
      pragas: r.pragas || r.praga || r.alvos || r['pragas_alvo'] || '',
      registro: r.registro || r.numero_registro || r['n_registro'] || '',
      empresa: r.empresa || r.titular_registro || r.registrante || '',
      formulacao: r.formulacao || r['formulação'] || r.form || '',
    })).filter(x => x.nome);

    res.set('Cache-Control', 'no-store');
    return res.json(mapped);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e.message || e) });
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`API ligada em http://localhost:${port}`);
});
