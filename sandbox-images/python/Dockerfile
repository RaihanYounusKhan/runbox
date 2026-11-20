# Python 3.13 (also provides bash for the "bash" language).
FROM python:3.13-alpine
RUN apk add --no-cache bash \
 && rm -rf /root/.cache /usr/local/lib/python3.13/ensurepip
# The worker forces this user anyway; set it here so the image is safe by default.
USER 65534:65534
WORKDIR /tmp
