FROM bitnami/minideb:stretch
MAINTAINER Adam Cribbs <adam.cribbs@ndorms.ox.ac.uk>
ENV SINGULARITY_VERSION=2.4.5
ADD . /tmp/repo
WORKDIR /tmp/repo
ENV PATH /opt/conda/bin:${PATH}
ENV LANG C.UTF-8
ENV SHELL /bin/bash
ADD conda/environments/cgat-core.yml /tmp/environment.yml
RUN install_packages wget bzip2 ca-certificates gnupg2 squashfs-tools git && \
    wget -O- http://neuro.debian.net/lists/xenial.us-ca.full > /etc/apt/sources.list.d/neurodebian.sources.list && \
    wget -O- http://neuro.debian.net/_static/neuro.debian.net.asc | apt-key add - && \
    install_packages singularity-container && \
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \
    rm Miniconda3-latest-Linux-x86_64.sh
RUN conda update -n base -c defaults conda && conda env create -f /tmp/environment.yml && conda clean --all -y && \
    pip install .