<template>
  <div class="min-h-screen bg-gray-50">
    <nav class="bg-white shadow-sm">
      <div class="container mx-auto px-4 py-3">
        
        <div class="flex justify-between items-center">
          
          <router-link to="/" class="flex items-center gap-3 hover:opacity-80 transition">
            <img src="/logo.png" alt="Logo Intuitive Care" class="h-8 w-auto" />
            <h1 class="text-xl font-bold text-gray-800 hidden md:block">
              Teste Intuitive Care | Luan Nascimento
            </h1>
          </router-link>

          <div class="space-x-4">
            <router-link to="/" class="text-gray-600 hover:text-blue-600 font-medium transition">
              Dashboard
            </router-link>
            <router-link to="/operadoras" class="text-gray-600 hover:text-blue-600 font-medium transition">
              Operadoras
            </router-link>
          </div>
          
        </div>
      </div>
    </nav>

    <main class="container mx-auto px-4 py-8">
      <router-view />
    </main>
  </div>
</template>

<script setup>
</script>