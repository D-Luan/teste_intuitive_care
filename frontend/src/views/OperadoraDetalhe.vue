<template>
  <div class="p-6">
    <div class="mb-6">
      <router-link to="/operadoras" class="text-blue-600 hover:text-blue-800 flex items-center">
        ← Voltar para lista de operadoras
      </router-link>
    </div>

    <div v-if="loading" class="text-center py-12">
      <div class="inline-block animate-spin rounded-full h-10 w-10 border-b-2 border-blue-700"></div>
      <p class="mt-2 text-gray-600">Carregando dados da operadora...</p>
    </div>

    <div v-else-if="error" class="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
      <p class="font-semibold">Erro ao carregar dados</p>
      <p>{{ error }}</p>
      <button @click="fetchOperadoraData" class="mt-2 px-4 py-2 bg-red-100 hover:bg-red-200 rounded text-red-700">
        Tentar novamente
      </button>
    </div>

    <div v-else class="space-y-8">
      <div class="bg-white rounded-lg shadow-md border border-gray-200 p-6">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-6">
          <div>
            <h1 class="text-2xl md:text-3xl font-bold text-gray-800">
              {{ operadora.razao_social || 'Nome não disponível' }}
            </h1>
            <p class="text-gray-600 mt-1" v-if="operadora.reg_ans">
              Registro ANS: <strong>{{ operadora.reg_ans }}</strong>
            </p>
          </div>
          <div class="mt-4 md:mt-0">
            <span v-if="operadora.uf" class="inline-block px-3 py-1 text-sm font-semibold rounded-full bg-blue-100 text-blue-800">
              {{ operadora.uf }}
            </span>
            <span v-else class="inline-block px-3 py-1 text-sm font-semibold rounded-full bg-gray-100 text-gray-800">
              UF não informada
            </span>
          </div>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div class="border border-gray-200 rounded-lg p-4">
            <h3 class="text-sm font-medium text-gray-500 mb-1">CNPJ</h3>
            <p class="font-mono text-lg font-semibold text-gray-800 break-all">
              {{ formatCNPJ(operadora.cnpj) || 'Não informado' }}
            </p>
          </div>
          <div class="border border-gray-200 rounded-lg p-4">
            <h3 class="text-sm font-medium text-gray-500 mb-1">Modalidade</h3>
            <p class="text-lg font-semibold text-gray-800">
              {{ operadora.modalidade || 'Não informado' }}
            </p>
          </div>
          <div class="border border-gray-200 rounded-lg p-4">
            <h3 class="text-sm font-medium text-gray-500 mb-1">Total de Despesas</h3>
            <p class="text-lg font-semibold" :class="totalDespesas > 0 ? 'text-green-700' : 'text-gray-500'">
              {{ formatCurrency(totalDespesas) }}
              <span v-if="totalDespesas === 0" class="text-sm font-normal block">(sem registros)</span>
            </p>
          </div>
        </div>
      </div>

      <div class="bg-white rounded-lg shadow-md border border-gray-200 overflow-hidden">
        <div class="px-6 py-4 border-b border-gray-200 bg-gray-50">
          <h2 class="text-xl font-bold text-gray-800">Histórico de Despesas</h2>
          <p class="text-xs text-gray-500 mt-1" v-if="despesas.length > 0">
            Mostrando {{ despesas.length }} período(s) registrado(s)
          </p>
        </div>
        
        <div v-if="despesas.length === 0" class="text-center py-12 text-gray-500">
          <p class="text-lg font-medium">Nenhuma despesa registrada</p>
        </div>
        
        <div v-else class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Ano
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Trimestre
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Valor das Despesas
                </th>
                <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Variação
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <tr v-for="(item, index) in despesasOrdenadas" :key="`${item.ano}-${item.trimestre}`" 
                  class="hover:bg-gray-50 transition-colors">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {{ item.ano }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                  {{ item.trimestre }}º Trimestre
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-900">
                  {{ formatCurrency(item.valor) }}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm">
                  <span :class="getVariationClass(item, index)" class="inline-flex items-center">
                    {{ calcularVariacao(item, index) }}
                    <span v-if="index > 0 && item.valor !== despesasOrdenadas[index-1].valor" 
                          class="ml-1 text-xs">
                      {{ item.valor > despesasOrdenadas[index-1].valor ? '↗' : '↘' }}
                    </span>
                  </span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import api from '@/services/api'

const route = useRoute()
const cnpj = route.params.cnpj

const operadora = ref({})
const despesas = ref([])
const loading = ref(true)
const error = ref(null)

const totalDespesas = computed(() => {
  return despesas.value.reduce((total, item) => total + parseFloat(item.valor || 0), 0)
})

const despesasOrdenadas = computed(() => {
  return [...despesas.value].sort((a, b) => {
    if (a.ano === b.ano) return b.trimestre - a.trimestre
    return b.ano - a.ano
  })
})

const formatCNPJ = (val) => {
  if (!val) return ''
  return val.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, '$1.$2.$3/$4-$5')
}

const formatCurrency = (value) => {
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL'
  }).format(value || 0)
}

const calcularVariacao = (item, index) => {
  if (index >= despesasOrdenadas.value.length - 1) return '—'

  const current = item.valor
  const previous = despesasOrdenadas.value[index + 1].valor
  
  if (previous === 0) return '—'
  
  const variacao = ((current - previous) / previous) * 100
  return `${variacao >= 0 ? '+' : ''}${variacao.toFixed(1)}%`
}

const getVariationClass = (item, index) => {
  if (typeof index === 'undefined' || index >= despesasOrdenadas.value.length - 1) {
    return 'text-gray-500'
  }
  
  const current = item.valor
  const previousItem = despesasOrdenadas.value[index + 1]
  
  if (!previousItem) return 'text-gray-500'

  const previous = previousItem.valor
  
  if (previous === current) return 'text-gray-500'
  return current > previous ? 'text-green-600 font-semibold' : 'text-red-600 font-semibold'
}

const fetchOperadoraData = async () => {
  loading.value = true
  error.value = null
  
  try {
    const [opRes, despRes] = await Promise.all([
      api.getOperadora(cnpj),
      api.getHistorico(cnpj)
    ])

    operadora.value = opRes.data
    despesas.value = despRes.data || []
    
  } catch (err) {
    console.error('Erro ao buscar detalhes:', err)
    error.value = 'Não foi possível carregar os dados desta operadora.'
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchOperadoraData()
})
</script>