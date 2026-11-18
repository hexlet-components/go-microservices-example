FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /audit ./cmd/audit && CGO_ENABLED=0 go build -o /probe ./cmd/probe

FROM scratch
COPY --from=build /audit /audit
COPY --from=build /probe /probe
USER 10001:10001
ENTRYPOINT ["/audit"]
