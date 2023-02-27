FROM alpine:latest as certificates
RUN apk --update add ca-certificates

FROM golang:1.20-buster AS builder
ARG USER_UID=10001
ARG GOOS=linux
ARG GOARCH=amd64
WORKDIR /dest
COPY . .
RUN make otelcontribcol && \
    mv ./bin/otelcontribcol_${GOOS}_${GOARCH} otelcontribcol && \
    chown ${USER_UID}:${USER_UID} otelcontribcol

FROM scratch
ARG USER_UID=10001
USER ${USER_UID}
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /dest/otelcontribcol /otelcontribcol
EXPOSE 4317 55680 55679
ENTRYPOINT ["/otelcontribcol"]
CMD ["--config", "/conf/collector.yaml"]
