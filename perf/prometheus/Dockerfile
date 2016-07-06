FROM fedora:23
RUN dnf -y install gettext hostname
ADD prometheus-0.20.0.linux-amd64.tar.gz /usr/local/
COPY prometheus.template /etc/prometheus/
COPY run_prometheus /

EXPOSE 9090
VOLUME [ "/prometheus" ]
WORKDIR /prometheus
CMD [ "/run_prometheus" ]
