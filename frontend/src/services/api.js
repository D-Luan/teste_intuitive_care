import axios from 'axios';

const api = axios.create({
  baseURL: 'http://127.0.0.1:8000/api',
  headers: {
    'Content-Type': 'application/json',
  }
});

export default {
  listarOperadoras(page = 1, limit = 10, search = '') {
    return api.get('/operadoras/', {
      params: { page, limit, search }
    });
  },
  getOperadora(cnpj) {
    return api.get(`/operadoras/${cnpj}`);
  },
  getHistorico(cnpj) {
    return api.get(`/operadoras/${cnpj}/despesas`);
  },
  
  getEstatisticas() {
    return api.get('/estatisticas/');
  }
};