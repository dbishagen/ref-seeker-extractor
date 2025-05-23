FROM golang:1.24.2-alpine

WORKDIR /usr/local/ref-seeker-client

COPY go.mod ./
RUN go mod tidy
COPY *.go ./
RUN go build -o /usr/local/bin/ref-seeker-client
COPY conf/ /usr/local/ref-seeker-client/conf/

ENTRYPOINT [ "/usr/local/bin/ref-seeker-client" ]
CMD [ "-i", "/usr/local/ref-seeker-client/conf/ind_dependencies.txt", "-d", "/usr/local/ref-seeker-client/conf/ref-seeker-db-configs.json", "-u", "http://ref-seeker-extractor:8001/", "-p" ]