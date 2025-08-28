ARG PYTHON_VER=3.12
ARG DEBIAN_VER=trixie
FROM python:${PYTHON_VER}-slim-${DEBIAN_VER}

RUN apt-get update &&\
    apt-get install -y --no-install-recommends build-essential gcc gdal-bin gdal-data libgdal-dev &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

RUN useradd -u 1001 -p oceanum --create-home --shell=/bin/bash oceanum

USER oceanum
WORKDIR /home/oceanum
ENV PIP_NO_CACHE_DIR=false
RUN python -m venv .venv/oceanum

# Set environment variables to activate the virtual environment globally
ENV VIRTUAL_ENV="/home/oceanum/.venv/oceanum"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
SHELL ["/bin/bash", "-c"]

# Install dependencies first
COPY --chown=oceanum:oceanum pyproject.toml /home/oceanum/oceanum-python/
RUN pip install -U pip pip-tools &&\
    pip-compile /home/oceanum/oceanum-python/pyproject.toml &&\
    pip install -r /home/oceanum/oceanum-python/requirements.txt

# Now copy the rest of the code
COPY --chown=oceanum:oceanum . /home/oceanum/oceanum-python/
WORKDIR /home/oceanum/oceanum-python
# Install the package (virtual environment is automatically activated via ENV variables)
RUN pip install -e .
CMD ["oceanum"]