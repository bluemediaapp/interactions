FROM golang:1.16
WORKDIR opt/
COPY go.* ./
RUN go mod download
COPY . ./
CMD ["sh", "run.sh"]
