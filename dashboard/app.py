import streamlit as st
import subprocess
import pandas as pd
import time
import os
from pandas.errors import EmptyDataError  # ← mudança

# ───── Placeholders para atualização em tempo real ─────
placeholder_table = st.empty()
placeholder_chart = st.empty()

st.title("Dashboard de Experimentos")

# Inputs básicos
k_max    = st.number_input(
    "Grau máximo de paralelismo",
    min_value=1, max_value=32,
    value=4, step=1
)
n_events = st.number_input(
    "Número de eventos",
    min_value=1000, max_value=1_000_000_000_000_000,
    value=50_000, step=1000
)
run_btn  = st.button("Iniciar experimento 🚀")

# Função para desenhar tabela + gráfico
def update_display(df):
    placeholder_table.dataframe(df)
    df_piv = df.pivot(index="k", columns="engine", values="seconds")
    placeholder_chart.line_chart(df_piv)

# 1) Ao clicar: dispara e marca estado de "running"
if run_btn:
    # remove benchmark antigo para não misturar resultados
    try:
        os.remove("data/benchmarks.csv")
    except FileNotFoundError:
        pass

    cmd = [
        "python", "controller/run_experiment.py",
        "--k-list", ",".join(str(i) for i in range(1, k_max+1)),
        "--samples", str(int(n_events))
    ]
    st.session_state.proc    = subprocess.Popen(cmd)
    st.session_state.running = True
    st.rerun()

# 2) Se estiver rodando, mostra spinner e atualiza em loop
if st.session_state.get("running", False):
    proc = st.session_state.proc

    with st.spinner("Executando experimento (aguarde)…"):
        # Enquanto o processo não terminar...
        while proc.poll() is None:
            # Se o CSV existe E não está vazio, tenta ler e exibir parcial
            bench_path = "data/benchmarks.csv"
            if os.path.exists(bench_path) and os.path.getsize(bench_path) > 0:  # ← mudança
                try:
                    df_partial = pd.read_csv(bench_path)                     # ← movimento 2
                    # filtra só até o k atual, evita runs antigas
                    df_partial = df_partial[df_partial["k"] <= k_max]       # ← mudança
                    update_display(df_partial)
                except EmptyDataError:
                    # ainda não escreveu header completo, ignora
                    pass
            time.sleep(1)

    # Processo terminou: limpa flag, pega saída e exibe logs
    st.session_state.running = False
    out, err = proc.communicate()
    if out:
        st.code(out, language="bash", label="Saída do controller")
    if err:
        st.error("🚨 Erro ao rodar controller:")
        st.code(err, language="bash")

# 3) Quando não estiver mais rodando, lê a saída definitiva e plota
if not st.session_state.get("running", False) and "proc" in st.session_state:
    bench_path = "data/benchmarks.csv"
    if os.path.exists(bench_path) and os.path.getsize(bench_path) > 0:  # ← mudança
        try:
            df = pd.read_csv(bench_path)                                 # ← movimento 3
            df = df[df["k"] <= k_max]                                   # mantém coerência com input
            st.success("✅ Experimento concluído!")
            st.subheader("Tempos por abordagem x k")
            update_display(df)
        except EmptyDataError:
            st.error("O arquivo de resultados está vazio ou corrompido.")
