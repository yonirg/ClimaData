import streamlit as st
import subprocess
import pandas as pd
import time
import os


st.title("Dashboard de Experimentos")

# Inputs básicos
k_max     = st.number_input("Grau máximo de paralelismo", min_value=1, max_value=32, value=4, step=1)
n_events  = st.number_input("Número de eventos",           min_value=1000, max_value=1_000_000, value=50_000, step=1000)
run_btn   = st.button("Iniciar experimento 🚀")

# 1) Ao clicar: dispara e marca estado de "running"
if run_btn:
    cmd = [
    "python", "controller/run_experiment.py",
    "--k-list", ",".join(str(i) for i in range(1, k_max+1)),
    "--samples", str(int(n_events))
    ]
    # dispara sem bloquear
    st.session_state.proc    = subprocess.Popen(cmd)
    st.session_state.running = True
    # força atualização imediata para entrar no loop abaixo
    st.rerun()

# 2) Se estiver rodando, mostra spinner e faz refresh periódico
if st.session_state.get("running", False):
    proc = st.session_state.proc

    # Enquanto o processo não terminar...
    if proc.poll() is None:
        with st.spinner("Executando experimento (aguarde)…"):
            # Apenas pausa um segundo para não spammer a CPU
            time.sleep(1)
        # Re-executa o script Streamlit do início, preservando session_state
        st.rerun()
    else:
        # Processo terminou: desabilita o flag e segue para exibir resultados
        st.session_state.running = False
        out, err = proc.communicate()
        if out:
            st.code(out, language="bash", label="Saída do controller")
        if err:
            st.error("🚨 Erro ao rodar controller:")
            st.code(err, language="bash")

# 3) Quando não estiver mais rodando, lê a saída e plota
if not st.session_state.get("running", False) and "proc" in st.session_state:
    # Exemplo: o controller grava benchmarks.csv em ./data
    df = pd.read_csv("data/benchmarks.csv")

    st.success("✅ Experimento concluído!")
    st.subheader("Tempos por abordagem x k")
    st.dataframe(df)

    st.subheader("Paralelismo vs Tempo")
    # aqui você pode usar plotly ou o próprio Streamlit
    df_piv = df.pivot(index="k", columns="engine", values="seconds")
    st.line_chart(df_piv)
