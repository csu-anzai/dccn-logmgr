# Compile
FROM golang:1.12-alpine AS compiler

RUN apk add --no-cache git

ARG LOGMGR_HUB_URL

# not in GOPATH, go modules auto enabled
WORKDIR /dccn-logmgr
COPY . .

RUN CGO_ENABLED=0 go vet  $(go list ./...| grep -v /example)
RUN CGO_ENABLED=0 go build -v -ldflags="-s -w \
                -X main.hubLogMgrAddr=${LOGMGR_HUB_URL}"

# Build image, alpine provides more possibilities than scratch
FROM alpine

COPY --from=compiler /dccn-logmgr/dccn-logmgr /dccn-logmgr
RUN ln -s /dccn-logmgr /usr/local/bin/dccn-logmgr

CMD ["dccn-logmgr"]
