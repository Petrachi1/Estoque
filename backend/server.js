// server.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import fetch from 'node-fetch';
import XLSX from 'xlsx';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());

// === 1) Servir FRONT (pasta /public)
const PUBLIC_DIR = path.join(__dirname, 'public');
app.use(express.static(PUBLIC_DIR));

// === 2) Endpoints de API (estoque & catálogo) ===================
async function getDropboxAccessToken() {
  const url = 'https://api.dropboxapi.com/oauth2/token';
  const body = new URLSearchParams({
    grant_type: 'refresh_token',
    refresh_token: process.env.DROPBOX_REFRESH_TOKEN,
    client_id: process.env.DROPBOX_APP_KEY,
    client_secret: process.env.DROPBOX_APP_SECRET,
  });
  const r = await fetch(url, { method: 'POST', body });
  if (!r.ok) throw new Error(`dropbox token ${r.status}: ${await r.text()}`);
  const j = await r.json();
  return j.access_token;
}
async function dropboxDownloadFile(dbxPath) {
  const token = await getDropboxAccessToken();
  const url = 'https://content.dropboxapi.com/2/files/download';
  const headers = {
    Authorization: `Bearer ${token}`,
    'Dropbox-API-Arg': JSON.stringify({ path: dbxPath }),
  };
  const r = await fetch(url, { method: 'POST', headers });
  if (!r.ok) throw new Error(`dropbox download ${r.status}: ${await r.text()}`);
  const buf = Buffer.from(await r.arrayBuffer());
  return buf;
}
function workbookToRows(buffer, sheetName, headerRow = 1) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  const ws = wb.Sheets[sheetName || wb.SheetNames[0]];
  if (!ws) throw new Error(`Aba não encontrada: ${sheetName}`);
  const range = XLSX.utils.decode_range(ws['!ref']);
  range.s.r = Math.max(0, Number(headerRow) - 1);
  ws['!ref'] = XLSX.utils.encode_range(range);
  return XLSX.utils.sheet_to_json(ws, { defval: '' });
}
function mapFields(row) {
  const get = (keys, def='') => { for (const k of keys) if (k in row && row[k] !== '') return String(row[k]).trim(); return def; };
  return {
    nome:      get(['nome_comercial','Nome Comercial','nome','Produto','produto']),
    qtd:       Number(get(['quantidade','qtd','estoque','saldo'],'0').replace(',','.')) || 0,
    und:       get(['unidade','un','und','u.m.']),
    ia:        get(['ia','ingrediente_ativo','ingrediente ativo','IAs']),
    pragas:    get(['pragas','praga','alvos','pragas_alvo']),
    registro:  get(['registro','numero_registro','n_registro','Nº Registro']),
    empresa:   get(['empresa','titular_registro','registrante']),
    formulacao:get(['formulacao','formulação','form']),
  };
}

app.get('/api/estoque', async (req, res) => {
  try {
    const buf = await dropboxDownloadFile(process.env.DROPBOX_FILE_PATH);
    const rows = workbookToRows(buf, process.env.EXCEL_SHEET, process.env.EXCEL_HEADER_ROW);
    const mapped = rows.map(mapFields).filter(x => x.nome);
    res.set('Cache-Control','no-store');
    res.json(mapped);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

app.get('/api/catalogo', async (req, res) => {
  try {
    let rows = [];
    if (process.env.CATALOGO_DROPBOX_PATH) {
      const buf = await dropboxDownloadFile(process.env.CATALOGO_DROPBOX_PATH);
      rows = workbookToRows(buf, null, 1);
    } else {
      // opcional: caminho local desabilitado em produção
      throw new Error('Defina CATALOGO_DROPBOX_PATH para produção');
    }
    const mapped = rows.map(r => ({
      nome: r.nome_comercial || r.nome || r.Produto || r.produto || r['Nome Comercial'] || '',
      ia: r.ia || r.ingrediente_ativo || r['ingrediente ativo'] || r.IAs || '',
      pragas: r.pragas || r.praga || r.alvos || r['pragas_alvo'] || '',
      registro: r.registro || r.numero_registro || r['n_registro'] || '',
      empresa: r.empresa || r.titular_registro || r.registrante || '',
      formulacao: r.formulacao || r['formulação'] || r.form || '',
    })).filter(x => x.nome);
    res.set('Cache-Control','no-store');
    res.json(mapped);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

// === 3) Fallback SPA/PWA (serve index.html em qualquer rota não-API)
app.get('*', (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'));
});

const port = process.env.PORT || 8080;
app.listen(port, () => console.log(`👉 http://localhost:${port}`));
