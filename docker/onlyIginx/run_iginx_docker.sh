#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
# TSIGinX@gmail.com
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#

ip=$1
name=$2
port=$3
logdir="$(pwd)/../../logs/docker_logs"
mkdir -p $logdir
docker run --name="${name}" --privileged -dit --net docker-cluster-iginx --ip ${ip} --add-host=host.docker.internal:host-gateway -v ${logdir}:/iginx/logs/ -p ${port}:6888 iginx:0.9.0-SNAPSHOT