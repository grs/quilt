FROM fedora:23
RUN dnf -y install initscripts fontconfig && dnf -y update && dnf clean all
ADD grafana-3.1.0-1466757358beta1.linux-x64.tar.gz /var/run/
COPY ./EnMasse-dashboard.json /var/run/grafana/dashboards
COPY ./defaults.ini /var/run/grafana-3.1.0-1466757358beta1/conf
RUN mkdir /var/run/grafana-3.1.0-1466757358beta1/data && mkdir /var/run/grafana-3.1.0-1466757358beta1/log
WORKDIR /var/run/grafana-3.1.0-1466757358beta1/
CMD ["/var/run/grafana-3.1.0-1466757358beta1/bin/grafana-server"]
