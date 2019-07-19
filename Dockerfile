FROM continuumio/miniconda3:latest

RUN conda install -y -c anaconda pandas beautifulsoup4 && \
	conda install -c conda-forge tqdm

WORKDIR /home/user/extradata

CMD ["/bin/bash"]