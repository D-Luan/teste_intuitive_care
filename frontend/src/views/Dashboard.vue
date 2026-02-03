<template>
  <div class="p-6">
    <h2 class="text-2xl font-bold text-gray-800 mb-6">Visão Geral das Despesas</h2>

    <div v-if="loading" class="text-center py-8">
      <div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-700"></div>
      <p class="mt-2 text-gray-600">Carregando dados...</p>
    </div>
    
    <div v-else-if="error" class="text-center py-8 text-red-600">
      Erro ao carregar dados: {{ error }}
    </div>

    <div v-else>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6 mb-8">
        <div class="bg-white p-4 md:p-6 rounded-lg shadow border border-gray-200 overflow-hidden">
          <h3 class="text-base md:text-lg font-semibold text-gray-600 mb-2">Total Geral</h3>
          <p class="text-xl md:text-2xl lg:text-3xl font-bold text-blue-700 break-words">
            {{ formatCurrency(estatisticas.total_geral) }}
          </p>
          <p class="text-xs md:text-sm text-gray-500 mt-2">Soma de todas as despesas registradas</p>
        </div>
        <div class="bg-white p-4 md:p-6 rounded-lg shadow border border-gray-200 overflow-hidden">
          <h3 class="text-base md:text-lg font-semibold text-gray-600 mb-2">Média por Operadora</h3>
          <p class="text-xl md:text-2xl lg:text-3xl font-bold text-green-700 break-words">
            {{ formatCurrency(estatisticas.media_geral) }}
          </p>
          <p class="text-xs md:text-sm text-gray-500 mt-2">Valor médio por operadora cadastrada</p>
        </div>
      </div>

      <div class="bg-white p-4 md:p-6 rounded-lg shadow border border-gray-200 mb-8">
        <h3 class="text-lg md:text-xl font-bold text-gray-800 mb-4">
          Distribuição de Despesas por Estado (UF)
        </h3>
        
        <div class="relative w-full h-105">
          <BarChart :chart-data="chartUfData" :options="chartOptions" />
        </div>
      </div>

      <div class="bg-white rounded-lg shadow border border-gray-200 overflow-hidden">
        <div class="px-4 md:px-6 py-4 border-b">
          <h3 class="text-lg md:text-xl font-bold text-gray-800">Ranking - Top 5 Operadoras</h3>
          <p class="text-xs md:text-sm text-gray-500 mt-1">
            Para detalhes completos, acesse a página "Operadoras"
          </p>
        </div>
        <div class="overflow-x-auto">
          <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
              <tr>
                <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Posição
                </th>
                <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Razão Social
                </th>
                <th class="px-4 md:px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Total de Despesas
                </th>
              </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
              <tr v-for="(op, index) in estatisticas.top_5" :key="index" class="hover:bg-gray-50">
                <td class="px-4 md:px-6 py-4 whitespace-nowrap">
                  <span class="inline-flex items-center justify-center w-6 h-6 rounded-full bg-blue-100 text-blue-800 text-sm font-medium">
                    {{ index + 1 }}
                  </span>
                </td>
                <td class="px-4 md:px-6 py-4 text-sm text-gray-700">
                  {{ op.razao_social }}
                </td>
                <td class="px-4 md:px-6 py-4 whitespace-nowrap text-sm font-semibold text-gray-900">
                  {{ formatCurrency(op.total) }}
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
import { ref, onMounted } from 'vue'
import BarChart from '@/components/BarChart.vue'
import api from '@/services/api'

const loading = ref(true)
const error = ref(null)

const estatisticas = ref({
  total_geral: 0,
  media_geral: 0,
  top_5: [],
  por_uf: []
})

const chartUfData = ref({
  labels: [],
  datasets: []
})

const chartOptions = ref({
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: { display: false },
    tooltip: {
      callbacks: {
        label: (context) => formatCurrency(context.raw)
      }
    }
  }
})

const formatCurrency = (value) => {
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value || 0)
}

const fetchEstatisticas = async () => {
  loading.value = true
  error.value = null
  try {
    const response = await api.getEstatisticas()
    const data = response.data
    
    estatisticas.value = data
    
    const labels = data.por_uf.map(item => item.uf)
    
    const values = data.por_uf.map(item => item.total)
    
    chartUfData.value = {
      labels: labels,
      datasets: [{
        label: 'Total de Despesas',
        data: values,
        backgroundColor: '#3B82F6',
        borderRadius: 4
      }]
    }

  } catch (err) {
    console.error(err)
    error.value = 'Falha ao carregar dados do dashboard.'
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  fetchEstatisticas()
})
</script>