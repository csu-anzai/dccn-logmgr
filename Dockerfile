# Compile
FROM golang:1.12-alpine AS compiler

RUN apk add --no-cache git

# not in GOPATH, go modules auto enabled
WORKDIR /dccn-es-api
COPY . .

RUN CGO_ENABLED=0 go vet ./...
RUN CGO_ENABLED=0 go build -v 

# Build image, alpine provides more possibilities than scratch
FROM alpine

COPY --from=compiler /dccn-es-api/dccn-es-api /dccn-es-api
RUN ln -s /dccn-es-api /usr/local/bin/dccn-es-api

CMD ["dccn-es-api"]
