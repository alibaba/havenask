FROM registry.cn-hangzhou.aliyuncs.com/havenask/os_base:1.0.0

# ha3_install
RUN mkdir -m 777 /ha3_install
ADD hape_tar.tar /ha3_install/
RUN sudo chmod -R 777 /ha3_install/hape_conf
RUN sudo chmod -R 777 /ha3_install/usr/local/share/zookeeper
RUN sudo ln -s /ha3_install/hape /usr/bin/hape
