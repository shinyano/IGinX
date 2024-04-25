ip=$1
name=$2
port=$3
docker run --name="${name}" --privileged -dit --net docker-cluster-iginx --ip ${ip} --add-host=host.docker.internal:host-gateway -p ${port}:6888 iginx:0.6.0