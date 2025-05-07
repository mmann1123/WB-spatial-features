FROM ubuntu:22.04

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget curl git bzip2 ca-certificates sudo libglib2.0-0 libxext6 libsm6 libxrender1 \
    libgl1-mesa-glx libglib2.0-dev libx11-dev build-essential \
    gdal-bin libgdal-dev python3-gdal && \
    rm -rf /var/lib/apt/lists/*
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install Miniconda
ENV CONDA_DIR=/opt/conda
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -p $CONDA_DIR && \
    rm /tmp/miniconda.sh && \
    $CONDA_DIR/bin/conda clean --all --yes
ENV PATH=$CONDA_DIR/bin:$PATH

# Install mamba
RUN conda install -y -c conda-forge mamba

# Create environment and install base packages
COPY spfeas_env_copy.yml /tmp/
# worked
RUN mamba env create -f /tmp/spfeas_env_copy.yml -y --verbose && \
    conda clean --all --yes


# Verify environment without activating
RUN /opt/conda/bin/conda run -n spfeas python -c 'import cython; print(cython.__version__)'

 
# Download and install mpglue
RUN wget https://github.com/jgrss/mpglue/archive/refs/tags/0.2.14.tar.gz -O mpglue-0.2.14.tar.gz
#print cython version

RUN /opt/conda/bin/conda run -n spfeas pip install mpglue-0.2.14.tar.gz


# # Clone and install spfeas
RUN git clone https://github.com/jgrss/spfeas.git /opt/spfeas && \
    /opt/conda/bin/conda run -n spfeas pip install /opt/spfeas

# Set default shell to bash
SHELL ["/bin/bash", "-c"]

# Activate environment by default
CMD ["bash"]
