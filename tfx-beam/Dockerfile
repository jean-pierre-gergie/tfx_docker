# ---- Base OS ----
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH

# ---- System deps ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bzip2 git build-essential \
    && rm -rf /var/lib/apt/lists/*

# ---- Miniconda install ----
ARG MINICONDA=Miniconda3-py310_24.3.0-0-Linux-x86_64.sh
RUN curl -fsSL https://repo.anaconda.com/miniconda/${MINICONDA} -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p ${CONDA_DIR} \
    && rm -f /tmp/miniconda.sh

# Ensure bash can use conda
SHELL ["/bin/bash", "-lc"]

# Accept Conda ToS if needed
RUN conda --version && (conda tos accept --yes --override-channels --channel https://repo.anaconda.com/pkgs/main --channel https://repo.anaconda.com/pkgs/r || true)

# ---- Create env and install Python ----
ARG ENV_NAME=tfx-spark-env
ARG PY_VER=3.10
RUN conda create -y -n ${ENV_NAME} python=${PY_VER} \
    && conda clean -afy

# ---- Install Python packages ----
ARG TFX_VER=1.16.0
ARG BEAM_VER=2.59.0
ARG JLAB_VER=3.6.7
ARG IPY_VER=7.34.0
ARG IPYW_VER=7.8.5

RUN source activate ${ENV_NAME} \
    && python -m pip install --no-cache-dir \
        "tfx==${TFX_VER}" \
        "apache-beam[gcp]==${BEAM_VER}" \
        "jupyterlab==${JLAB_VER}" \
        "ipython==${IPY_VER}" \
        "ipywidgets==${IPYW_VER}"

# ---- Create dev user & workspace ----
RUN useradd -m -s /bin/bash dev
USER dev
WORKDIR /workspace

# Expose Jupyter port (compose will run it)
EXPOSE 8888
