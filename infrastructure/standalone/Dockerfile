FROM jupyter/pyspark-notebook:spark-3.5.0

# Install uv for fast package management
# COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv
# ENV UV_SYSTEM_PYTHON=1

WORKDIR /home/jovyan

RUN pip install marimo

# Copy requirements file
# COPY --link requirements.txt .

# Install the requirements using uv
# RUN uv pip install -r requirements.txt

# Copy application files
# COPY --link app.py .
# Uncomment the following line if you need to copy additional files
# COPY --link . .

# EXPOSE 9999

# Create a non-root user and switch to it
# RUN useradd -m app_user
# USER app_user

# CMD [ "marimo", "edit", "--port", "8899", "--headless", "--no-token"]
