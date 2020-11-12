FROM python:3.7-slim as base

# ---- compile image -----------------------------------------------
FROM base AS compile-image
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential \
  gcc

RUN python -m venv /app/env
ENV PATH="/app/env/bin:$PATH"

COPY requirements.unexpanded.txt .
RUN pip install --upgrade pip
# pip install is fast here (while slow without the venv) :
RUN pip install -r requirements.unexpanded.txt

# ---- build image -----------------------------------------------
FROM base AS build-image
COPY --from=compile-image /app/env /app/env

# Make sure we use the virtualenv:
ENV PATH="/app/env/bin:$PATH"
COPY . /app
WORKDIR /app

EXPOSE 8080

CMD exec gunicorn --bind :$PORT main:app --timeout 240
