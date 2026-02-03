import { createRouter, createWebHistory } from 'vue-router'
import Dashboard from './views/Dashboard.vue'
import OperadorasList from './views/OperadorasList.vue'
import OperadoraDetalhe from './views/OperadoraDetalhe.vue'

const routes = [
  { path: '/', component: Dashboard },
  { 
    path: '/operadoras', 
    component: OperadorasList,
    name: 'operadoras' 
  },
  { path: '/operadora/:cnpj', component: OperadoraDetalhe, props: true }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router