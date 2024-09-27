# append to https://github.com/kneu-messenger-pigeon/github-workflows/blob/main/Dockerfile
# see https://github.com/kneu-messenger-pigeon/github-workflows/blob/main/.github/workflows/build.yaml#L20
ENV STORAGE_FILE /storage/storage.json
VOLUME /storage
RUN mkdir /storage && touch /storage/storage.json && chmod 777 -R /storage
