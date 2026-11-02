FROM busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662
COPY generate.sh /usr/local/bin/logister-generate
RUN sed -i 's/\r$//' /usr/local/bin/logister-generate && chmod 755 /usr/local/bin/logister-generate
ENTRYPOINT ["sh", "/usr/local/bin/logister-generate"]
