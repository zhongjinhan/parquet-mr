# Used to generate the docker image docker-registry.infra.cloudera.com/parquet/pre-commit referenced
# in pre_commit_hook.sh.
#
# Use the following commands to generate a new image and upload to the repo:
# > docker build -t docker-registry.infra.cloudera.com/parquet/pre-commit {directory of this file}
# > docker push docker-registry.infra.cloudera.com/parquet/pre-commit

FROM docker-registry.infra.cloudera.com/cauldron-testing/ubuntu1804

ARG jdk_url=https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
ARG jdk_target=/opt/openjdk-11
RUN wget -O /tmp/jdk.tgz $jdk_url && mkdir -p $jdk_target && tar zxf /tmp/jdk.tgz --strip-components 1 -C $jdk_target && rm -f /tmp/jdk.tgz

ENV OPENJDK_11_HOME=/opt/openjdk-11

CMD bash
