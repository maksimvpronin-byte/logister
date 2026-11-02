FROM golang:1.22-alpine@sha256:1699c10032ca2582ec89a24a1312d986a3f094aed3d5c1147b19880afe40e052 AS supervisor
WORKDIR /src
COPY supervisor/ ./
RUN CGO_ENABLED=0 go test ./... && CGO_ENABLED=0 go build -trimpath -o /out/shipper-supervisor .
FROM busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662 AS config
WORKDIR /config
COPY fluent-bit.conf parsers.conf ./
RUN sed -i 's/\r$//' *.conf
FROM fluent/fluent-bit:3.2.2@sha256:b7a2b9ee1958607da9a16b0efeb24a70a43c5b7e91dbabca4dc02e916aadb914
COPY --from=config /config/ /fluent-bit/etc/
COPY --from=supervisor /out/shipper-supervisor /usr/local/bin/shipper-supervisor
ENTRYPOINT ["/usr/local/bin/shipper-supervisor"]
CMD ["/fluent-bit/bin/fluent-bit", "-c", "/fluent-bit/etc/fluent-bit.conf"]
