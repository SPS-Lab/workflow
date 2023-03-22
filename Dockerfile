FROM nvidia/cuda:11.2.0-devel-ubuntu18.04
RUN mkdir -p /run/systemd && echo 'docker' > /run/systemd/container
ENV GPU_LIBRARY_PATH /usr/local/cuda/lib64
ENV GPU_INCLUDE_PATH /usr/local/cuda/include
RUN apt-get update && apt-get install -y git
RUN cd /home && git clone --depth 1 https://github.com/SPS-Lab/workflow.git \
    && cd workflow \
    && gcc cachetest.cpp \
    && cp ./a.out /home/.
