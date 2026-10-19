/**
 * Frontend JavaScript del Simulador de Apache Spark (ProcDatos).
 * Maneja el polling del estado del clúster, la renderización reactiva de nodos,
 * el trazado de tareas en tiempo real y los gráficos de telemetría con Chart.js.
 */

let cpuChart = null;
let ramChart = null;
let isPlaying = false;
let pollingInterval = null;

// ==========================================================================
// Inicialización
// ==========================================================================
document.addEventListener("DOMContentLoaded", () => {
  initCharts();
  setupEventListeners();
  fetchState();
  // Polling regular cada 300ms para mantener el dashboard sincronizado
  pollingInterval = setInterval(fetchState, 300);
});

function setupEventListeners() {
  document.getElementById("btn-play").addEventListener("click", togglePlay);
  document.getElementById("btn-step").addEventListener("click", stepOnce);
  document.getElementById("btn-reset").addEventListener("click", resetCluster);
  document.getElementById("speed-select").addEventListener("change", (e) => {
    fetch("/api/speed", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ speed_sec: parseFloat(e.target.value) })
    });
  });
}

// ==========================================================================
// Configuración de Gráficos (Chart.js)
// ==========================================================================
function initCharts() {
  const commonOptions = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 0 },
    scales: {
      x: {
        grid: { color: "rgba(51, 65, 85, 0.3)" },
        ticks: { color: "#64748b", font: { size: 10 } }
      },
      y: {
        grid: { color: "rgba(51, 65, 85, 0.3)" },
        ticks: { color: "#64748b", font: { size: 10 } },
        beginAtZero: true
      }
    },
    plugins: {
      legend: {
        labels: { color: "#cbd5e1", font: { size: 11 } }
      }
    }
  };

  const cpuCtx = document.getElementById("cpuChart").getContext("2d");
  cpuChart = new Chart(cpuCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label: "Uso CPU Clúster (%)",
        data: [],
        borderColor: "#f97316",
        backgroundColor: "rgba(249, 115, 22, 0.1)",
        fill: true,
        tension: 0.2,
        pointRadius: 0
      }]
    },
    options: {
      ...commonOptions,
      scales: {
        ...commonOptions.scales,
        y: { ...commonOptions.scales.y, max: 100 }
      }
    }
  });

  const ramCtx = document.getElementById("ramChart").getContext("2d");
  ramChart = new Chart(ramCtx, {
    type: "line",
    data: {
      labels: [],
      datasets: [{
        label: "RAM en Uso (MB)",
        data: [],
        borderColor: "#38bdf8",
        backgroundColor: "rgba(56, 189, 248, 0.1)",
        fill: true,
        tension: 0.2,
        pointRadius: 0
      }]
    },
    options: commonOptions
  });
}

// ==========================================================================
// Comunicación con la API REST
// ==========================================================================
async function fetchState() {
  try {
    const res = await fetch("/api/state");
    if (!res.ok) return;
    const state = await res.json();
    renderCluster(state);
  } catch (err) {
    console.error("Error al obtener estado:", err);
  }
}

async function togglePlay() {
  const newPlayState = !isPlaying;
  const res = await fetch("/api/play", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ play: newPlayState })
  });
  const data = await res.json();
  updatePlayButton(data.is_playing);
}

async function stepOnce() {
  await fetch("/api/step", { method: "POST" });
  fetchState();
}

async function resetCluster() {
  await fetch("/api/reset", { method: "POST" });
  updatePlayButton(false);
  fetchState();
}

async function killWorker(workerId) {
  await fetch("/api/worker/kill", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ worker_id: workerId })
  });
  fetchState();
}

async function reviveWorker(workerId) {
  await fetch("/api/worker/revive", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ worker_id: workerId })
  });
  fetchState();
}

async function loadScenario(scenarioId) {
  await fetch("/api/scenario/load", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scenario_id: scenarioId })
  });
  fetchState();
}

async function submitCustomJob() {
  const jobType = document.getElementById("job-type").value;
  const userId = document.getElementById("user-id").value;
  const dataSize = parseFloat(document.getElementById("data-size").value);
  const partitions = parseInt(document.getElementById("num-partitions").value);
  const skew = parseFloat(document.getElementById("skew-factor").value);

  await fetch("/api/submit-job", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: jobType,
      user_id: userId,
      data_size_mb: dataSize,
      partitions: partitions,
      skew_factor: skew
    })
  });
  fetchState();
}

function updateJobDefaults() {
  const type = document.getElementById("job-type").value;
  const userInput = document.getElementById("user-id");
  const sizeInput = document.getElementById("data-size");
  const partsInput = document.getElementById("num-partitions");
  const skewInput = document.getElementById("skew-factor");

  if (type === "analyst") {
    userInput.value = "analyst_maria";
    sizeInput.value = "80";
    partsInput.value = "4";
    skewInput.value = "1.0";
  } else if (type === "etl") {
    userInput.value = "engineer_carlos";
    sizeInput.value = "350";
    partsInput.value = "8";
    skewInput.value = "1.0";
  } else if (type === "ml") {
    userInput.value = "scientist_laura";
    sizeInput.value = "160";
    partsInput.value = "4";
    skewInput.value = "1.0";
  } else if (type === "burst") {
    userInput.value = "multiuser_burst";
    sizeInput.value = "300";
    partsInput.value = "8";
    skewInput.value = "1.0";
  }
}

// ==========================================================================
// Renderizado Reactivo del Clúster
// ==========================================================================
function renderCluster(state) {
  // 1. Tick y Controles
  document.getElementById("current-tick").innerText = state.current_tick;
  updatePlayButton(state.is_playing);

  // 2. Resumen Superior
  document.getElementById("cores-summary").innerText = 
    `${state.metrics.cluster_cores_used} / ${state.metrics.cluster_cores_total}`;
  document.getElementById("ram-summary").innerText = 
    `${Math.round(state.metrics.cluster_ram_used_mb)} / ${Math.round(state.metrics.cluster_ram_total_mb)} MB`;
  document.getElementById("driver-jobs-count").innerText = state.driver.total_jobs_count;

  // 3. Renderizar Nodos Worker
  const workersGrid = document.getElementById("workers-grid");
  workersGrid.innerHTML = state.workers.map(w => {
    const isDead = w.status === "DEAD";
    return `
      <div class="worker-card ${isDead ? 'dead' : ''}">
        <div class="worker-header">
          <div class="worker-title">
            <i class="fa-solid fa-server"></i> ${w.worker_id}
          </div>
          <span class="badge-status ${isDead ? 'badge-dead' : 'badge-healthy'}">
            ${isDead ? 'FALLIDO' : 'SALUDABLE'}
          </span>
        </div>

        <div class="metric-row">
          <div class="metric-label-row">
            <span>Cores (${w.cores_used}/${w.cores_total})</span>
            <span>${w.cpu_utilization_pct}%</span>
          </div>
          <div class="bar-bg">
            <div class="bar-fill-cpu" style="width: ${w.cpu_utilization_pct}%;"></div>
          </div>
        </div>

        <div class="metric-row">
          <div class="metric-label-row">
            <span>RAM (${Math.round(w.total_used_ram_mb)} / ${Math.round(w.total_ram_mb)} MB)</span>
            <span>${w.ram_utilization_pct}%</span>
          </div>
          <div class="bar-bg">
            <div class="bar-fill-ram" style="width: ${w.ram_utilization_pct}%;"></div>
          </div>
        </div>

        <div style="font-size: 0.7rem; color: #94a3b8; display: flex; justify-content: space-between;">
          <span>Tareas activas: <strong>${w.active_tasks_count}</strong></span>
          <span>Particiones RAM: <strong>${w.cached_partitions_count}</strong></span>
        </div>

        <div class="worker-actions">
          ${isDead 
            ? `<button class="btn-worker-revive" onclick="reviveWorker('${w.worker_id}')"><i class="fa-solid fa-heart-pulse"></i> Revivir</button>` 
            : `<button class="btn-worker-kill" onclick="killWorker('${w.worker_id}')"><i class="fa-solid fa-skull"></i> Matar Nodo</button>`}
        </div>
      </div>
    `;
  }).join("");

  // 4. Renderizar Cola FIFO
  const queueList = document.getElementById("queue-list");
  document.getElementById("queue-count").innerText = state.queue.length;
  if (state.queue.length === 0) {
    queueList.innerHTML = `<p class="empty-state">No hay trabajos en espera. El clúster está libre.</p>`;
  } else {
    queueList.innerHTML = state.queue.map((q, idx) => `
      <div class="queue-item">
        <span><strong>#${idx + 1}</strong> [${q.user_id}] ${q.name}</span>
        <span style="color: #38bdf8;">${q.total_tasks} tareas</span>
      </div>
    `).join("");
  }

  // 5. Renderizar Trabajo Activo (Spark UI)
  const activeJob = state.active_job;
  const badge = document.getElementById("active-job-badge");

  if (!activeJob) {
    badge.className = "badge badge-inactive";
    badge.innerText = "SIN TRABAJO ACTIVO";
    document.getElementById("job-name").innerText = "-";
    document.getElementById("job-user").innerText = "-";
    document.getElementById("job-action").innerText = "-";
    document.getElementById("job-duration").innerText = "0 ticks";
    document.getElementById("job-progress-pct").innerText = "0%";
    document.getElementById("job-progress-fill").style.width = "0%";
    document.getElementById("stages-list").innerHTML = "<p class='empty-state'>En espera de acción Spark...</p>";
    document.getElementById("tasks-grid").innerHTML = "";
  } else {
    badge.className = "badge badge-healthy";
    badge.innerText = `JOB #${activeJob.job_id} EN EJECUCIÓN`;
    document.getElementById("job-name").innerText = activeJob.name;
    document.getElementById("job-user").innerText = activeJob.user_id;
    document.getElementById("job-action").innerText = activeJob.action_name;
    document.getElementById("job-duration").innerText = `${activeJob.duration_ticks} ticks`;
    document.getElementById("job-progress-pct").innerText = `${activeJob.progress_pct}%`;
    document.getElementById("job-progress-fill").style.width = `${activeJob.progress_pct}%`;

    // Renderizar Stages
    const stagesContainer = document.getElementById("stages-list");
    stagesContainer.innerHTML = activeJob.stages.map(st => `
      <div class="stage-card ${st.is_shuffle_map ? 'shuffle' : 'result'}">
        <div>
          <strong>${st.name}</strong> 
          <span style="color: #64748b; font-size: 0.7rem;">(${st.is_shuffle_map ? 'Escribe Shuffle para siguiente etapa' : 'Produce resultado al Driver'})</span>
        </div>
        <div>
          <span>${st.completed_tasks} / ${st.total_tasks} tareas (${st.progress_pct}%)</span>
        </div>
      </div>
    `).join("");

    // Renderizar Tareas
    const tasksGrid = document.getElementById("tasks-grid");
    const allTasks = activeJob.stages.flatMap(st => st.tasks);
    tasksGrid.innerHTML = allTasks.map(t => {
      let statusClass = "pending";
      if (t.is_recomputation) statusClass = "recomputing";
      else if (t.status === "RUNNING") statusClass = "running";
      else if (t.status === "SUCCESS") statusClass = "success";

      return `
        <div class="task-cell ${statusClass}" title="Tarea #${t.task_id} | Partición: ${t.partition_index} | Worker: ${t.worker_id || 'N/A'}">
          <div>T#${t.task_id}</div>
          <div>${t.status === 'RUNNING' ? t.progress_pct + '%' : t.status.substring(0, 4)}</div>
        </div>
      `;
    }).join("");
  }

  // 6. KPIs y Resiliencia
  document.getElementById("kpi-shuffle").innerText = `${state.metrics.total_shuffle_mb} MB`;
  document.getElementById("kpi-recomputed").innerText = state.resilience.total_tasks_retried;
  document.getElementById("kpi-completed-jobs").innerText = state.metrics.completed_jobs_count;

  // 7. Actualizar Gráficos
  updateCharts(state.metrics.history);

  // 8. Logs de Eventos
  const logsContainer = document.getElementById("event-logs");
  logsContainer.innerHTML = state.events.map(ev => `
    <div class="log-entry">
      <span class="log-tick">[Tick ${ev.tick}]</span>
      <span class="log-type ${ev.type}">${ev.type}</span>
      <span class="log-msg">${ev.message}</span>
    </div>
  `).join("");
}

function updateCharts(history) {
  if (!history || !history.ticks || history.ticks.length === 0) return;

  const labels = history.ticks.map(t => `T-${t}`);
  
  cpuChart.data.labels = labels;
  cpuChart.data.datasets[0].data = history.cpu;
  cpuChart.update();

  ramChart.data.labels = labels;
  ramChart.data.datasets[0].data = history.ram;
  ramChart.update();
}

function updatePlayButton(playing) {
  isPlaying = playing;
  const btn = document.getElementById("btn-play");
  const txt = document.getElementById("play-text");
  if (playing) {
    btn.className = "btn btn-danger";
    btn.innerHTML = `<i class="fa-solid fa-pause"></i> <span>Pausar</span>`;
  } else {
    btn.className = "btn btn-primary";
    btn.innerHTML = `<i class="fa-solid fa-play"></i> <span>Iniciar</span>`;
  }
}
