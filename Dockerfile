FROM golang:1.20-alpine

ENV CGO_ENABLED=0
# Install basic packages. use based on need.
# RUN apt update
# RUN apt install -y openssl ca-certificates vim make gcc golang-go protobuf-compiler python3 netcat iputils-ping iproute2

# # Set up certificates
# ARG cert_location=/usr/local/share/ca-certificates
# RUN mkdir -p ${cert_location}
# # Get certificate from "github.com"
# RUN openssl s_client -showcerts -connect github.com:443 </dev/null 2>/dev/null|openssl x509 -outform PEM > ${cert_location}/github.crt
# # Get certificate from "proxy.golang.org"
# RUN openssl s_client -showcerts -connect google.golang.org:443 </dev/null 2>/dev/null|openssl x509 -outform PEM >  ${cert_location}/google.golang.crt
# # Update certificates
# RUN update-ca-certificates

# Install go extensions for protoc
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
ENV PATH="$PATH:/go/bin"


#copying the project to docker

COPY chat-system /app/chat-system
ENV APP_HOME /app/chat-system
WORKDIR "${APP_HOME}"



#build the go project
RUN go mod init chat-system
RUN go mod tidy
RUN go mod verify
WORKDIR "${APP_HOME}"

#install dependencies
RUN go mod download

EXPOSE 12000

#build the server
WORKDIR "$APP_HOME/cmd/server"
RUN go build -o "$APP_HOME/server"
WORKDIR "${APP_HOME}"

#build the client
RUN go build -o "client"

ENTRYPOINT /bin/sh


