<template>
  <div class="p-6">
    <div class="flex justify-between items-center mb-6">
      <div>
        <h1 class="text-2xl font-bold text-gray-800">Operadoras de Saúde</h1>
        <p class="text-gray-600">Lista completa de operadoras cadastradas na ANS</p>
      </div>
      <div class="text-right">
        <div class="text-sm text-gray-500">Total de registros</div>
        <div class="text-2xl font-bold text-blue-700">{{ totalRegistros }}</div>
      </div>
    </div>

    <div class="mb-6">
      <div class="relative">
        <input
          v-model="searchTerm"
          @input="onSearchInput"
          type="text"
          placeholder="Buscar por Razão Social ou CNPJ..."
          class="w-full p-4 pl-12 border border-gray-300 rounded-lg shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
        >
        <div v-if="searchTerm" class="absolute right-4 top-4">
          <button @click="clearSearch" class="text-gray-400 hover:text-gray-600">
            ✕
          </button>
        </div>
      </div>
      <p class="text-sm text-gray-500 mt-2">
        A busca é feita no servidor. Digite pelo menos 3 caracteres para filtrar.
      </p>
    </div>

    <div v-if="loading" class="text-center py-12">
      <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-700"></div>
      <p class="mt-2 text-gray-600">Carregando operadoras...</p>
    </div>

    <div v-else-if="error" class="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
      <p class="font-semibold">Erro ao carregar dados</p>
      <p>{{ error }}</p>
      <button @click="fetchOperadoras" class="mt-2 px-4 py-2 bg-red-100 hover:bg-red-200 rounded text-red-700">
        Tentar novamente
      </button>
    </div>

    <div v-else>
      <div class="bg-white rounded-lg shadow overflow-hidden border border-gray-200">
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  CNPJ
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Razão Social
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Registro ANS
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  UF
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Modalidade
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Ações
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <tr v-for="operadora in operadoras" :key="operadora.cnpj" class="hover:bg-gray-50 transition">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-mono text-gray-900">
                  {{ formatCNPJ(operadora.cnpj) }}
                </td>
                <td class="px-6 py-4">
                  <div class="text-sm font-medium text-gray-900">{{ operadora.razao_social }}</div>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ operadora.reg_ans }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap">
                  <span class="px-2 py-1 text-xs font-semibold rounded-full bg-blue-100 text-blue-800">
                    {{ operadora.uf || 'N/A' }}
                  </span>
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {{ operadora.modalidade || 'Não informado' }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                  <router-link
                    :to="`/operadora/${operadora.cnpj}`"
                    class="text-blue-600 hover:text-blue-900 hover:underline"
                  >
                    Ver detalhes →
                  </router-link>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div v-if="operadoras.length === 0" class="text-center py-8 text-gray-500">
          Nenhuma operadora encontrada com os filtros atuais.
        </div>
      </div>

      <div class="mt-6 px-2">
        <div class="flex flex-col md:grid md:grid-cols-3 md:items-center gap-4">
          
          <div class="hidden md:block"></div>
          
          <div class="flex justify-center space-x-2">
            <button
              @click="prevPage"
              :disabled="currentPage === 1"
              class="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Anterior
            </button>
            
            <div class="flex items-center bg-white border border-gray-300 rounded-md px-2">
              <span class="text-sm text-gray-700 mr-2">Página</span>
              <input
                v-model.number="inputPage"
                @keyup.enter="goToPage"
                type="number"
                min="1"
                :max="totalPages"
                class="w-12 text-center py-1 text-sm outline-none"
              >
              <span class="text-sm text-gray-500 ml-2 border-l pl-2">de {{ totalPages }}</span>
            </div>
            
            <button
              @click="nextPage"
              :disabled="currentPage >= totalPages"
              class="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Próxima
            </button>
          </div>
          
          <div class="flex justify-center md:justify-end items-center">
            <span class="text-sm text-gray-700 mr-2">Itens por página:</span>
            <select
              v-model="itemsPerPage"
              @change="onItemsPerPageChange"
              class="border border-gray-300 rounded py-1 px-2 text-sm bg-white cursor-pointer hover:border-blue-400 focus:ring-2 focus:ring-blue-500 outline-none"
            >
              <option value="5">5</option>
              <option value="10">10</option>
              <option value="20">20</option>
              <option value="50">50</option>
            </select>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import api from '@/services/api'

const operadoras = ref([])
const loading = ref(false)
const error = ref(null)
const currentPage = ref(1)
const totalRegistros = ref(0)
const itemsPerPage = ref(10)
const searchTerm = ref('')
const inputPage = ref(1)

let searchTimeout = null

const totalPages = computed(() => {
  return Math.ceil(totalRegistros.value / itemsPerPage.value) || 1
})

const formatCNPJ = (cnpj) => {
  if (!cnpj) return ''
  return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5')
}

const fetchOperadoras = async () => {
  loading.value = true
  error.value = null
  
  try {
    const response = await api.listarOperadoras(
      currentPage.value, 
      itemsPerPage.value, 
      searchTerm.value
    )
    
    const resultado = response.data
    operadoras.value = resultado.data
    totalRegistros.value = resultado.total
    inputPage.value = currentPage.value
    
  } catch (err) {
    console.error('Erro ao buscar operadoras:', err)
    error.value = 'Não foi possível conectar ao servidor.'
    operadoras.value = []
  } finally {
    loading.value = false
  }
}

const onSearchInput = () => {
  clearTimeout(searchTimeout)
  searchTimeout = setTimeout(() => {
    currentPage.value = 1
    fetchOperadoras()
  }, 500)
}

const clearSearch = () => {
  searchTerm.value = ''
  currentPage.value = 1
  fetchOperadoras()
}

const nextPage = () => {
  if (currentPage.value < totalPages.value) {
    currentPage.value++
    fetchOperadoras()
  }
}

const prevPage = () => {
  if (currentPage.value > 1) {
    currentPage.value--
    fetchOperadoras()
  }
}

const goToPage = () => {
  const page = parseInt(inputPage.value)
  if (page >= 1 && page <= totalPages.value) {
    currentPage.value = page
    fetchOperadoras()
  } else {
    inputPage.value = currentPage.value
  }
}

const onItemsPerPageChange = () => {
  currentPage.value = 1
  fetchOperadoras()
}

onMounted(() => {
  fetchOperadoras()
})
</script>

<style scoped>
table {
  border-collapse: separate;
  border-spacing: 0;
}

th {
  position: sticky;
  top: 0;
  background-color: #f9fafb;
  z-index: 10;
}

tr:hover {
  background-color: #f9fafb;
}

input[type="number"]::-webkit-inner-spin-button,
input[type="number"]::-webkit-outer-spin-button {
  -webkit-appearance: none;
  margin: 0;
}

input[type="number"] {
  -moz-appearance: textfield;
}
</style>