ARG GO_VERSION=${GO_VERSION:-1.19}

FROM golang:${GO_VERSION}-alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR /src
COPY ./go.mod ./go.sum ./
RUN go mod download

COPY . .
RUN cat /etc/passwd | grep nobody > /etc/passwd.nobody

# Build the binary.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -tags=nomsgpack -o /app .

# build a small image
FROM alpine

RUN apk add tzdata

ENV STORAGE_FILE /storage.txt
RUN touch "${STORAGE_FILE}" && chmod 777 "${STORAGE_FILE}"

COPY --from=builder /etc/passwd.nobody /etc/passwd
COPY --from=builder /app /app

# Run
USER nobody
ENTRYPOINT ["/app"]
