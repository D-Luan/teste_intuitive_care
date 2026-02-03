<template>
  <div class="relative w-full h-full">
    <canvas ref="chartCanvas" class="w-full h-full"></canvas>
  </div>
</template>

<script>
import { Chart, registerables } from 'chart.js';
import { ref, onMounted, watch } from 'vue';

Chart.register(...registerables);

export default {
  name: 'BarChart',
  props: {
    chartData: {
      type: Object,
      required: true
    },
    chartOptions: {
      type: Object,
      default: () => ({})
    }
  },
  setup(props) {
    const chartCanvas = ref(null);
    let chartInstance = null;

    const renderChart = () => {
      if (chartInstance) {
        chartInstance.destroy();
      }

      if (chartCanvas.value && props.chartData) {
        const ctx = chartCanvas.value.getContext('2d');
        chartInstance = new Chart(ctx, {
          type: 'bar',
          data: props.chartData,
          options: props.chartOptions
        });
      }
    };

    onMounted(() => {
      renderChart();
    });

    watch(() => props.chartData, () => {
      renderChart();
    }, { deep: true });

    watch(() => props.chartOptions, () => {
      renderChart();
    }, { deep: true });

    return {
      chartCanvas
    };
  }
};
</script>