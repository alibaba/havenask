FROM registry.cn-hangzhou.aliyuncs.com/havenask/os_base:1.0.0-beta
RUN cd /usr/bin && wget https://releases.bazel.build/5.2.0/release/bazel-5.2.0-linux-x86_64 -O bazel  && chmod +x bazel
RUN mkdir /external
COPY external /external
