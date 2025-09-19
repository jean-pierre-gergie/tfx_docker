FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Pin numpy < 2.0 to avoid np.string_ error in TB 2.16
RUN pip install --no-cache-dir "numpy<2" "tensorboard==2.16.*"

ENV TB_LOGDIR=/artifacts \
    TB_PORT=6006 \
    TB_HOST=0.0.0.0

VOLUME ["/artifacts"]
WORKDIR /app
EXPOSE 6006

CMD ["bash", "-lc", "tensorboard --logdir=${TB_LOGDIR} --host=${TB_HOST} --port=${TB_PORT}"]
