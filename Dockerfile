FROM golang:14
COPY go.* ./
RUN go mod download
WORKDIR opt/
COPY . ./
CMD ["sh", "run.sh"]