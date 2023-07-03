# append to https://github.com/kneu-messenger-pigeon/github-workflows/blob/main/Dockerfile
# see https://github.com/kneu-messenger-pigeon/github-workflows/blob/main/.github/workflows/build.yaml#L20
ENV STORAGE_FILE /storage/storage.txt
VOLUME /storage
RUN touch /storage/storage.txt && chmod 777 /storage/storage.txt
