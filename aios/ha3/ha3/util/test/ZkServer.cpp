#include <ha3/util/test/ZkServer.h>
#include <ha3/test/test.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <unistd.h>

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, ZkServer);

ZkServer::ZkServer() {
    start();
}

ZkServer::~ZkServer() { 
    stop();
}

ZkServer* ZkServer::getZkServer() {
    static ZkServer me;
    return &me;
}

unsigned short ZkServer::start() {
    stop();
    _port = choosePort();
    std::ostringstream oss;
    oss << TEST_ZOOKEEPER_PATH << "/start.sh " << _port << " " << getpid();
    system(oss.str().c_str());
    return _port;
} 

unsigned short ZkServer::simpleStart() {
    stop();
    std::ostringstream oss;
    oss << TEST_ZOOKEEPER_PATH << "/start.sh " << _port << " " << getpid();
    system(oss.str().c_str());
    return _port;
} 

void ZkServer::stop() {
    std::ostringstream oss;
    oss << TEST_ZOOKEEPER_PATH << "/stop.sh " << getpid();
    system(oss.str().c_str());
}

unsigned short ZkServer::getPort(unsigned short from, unsigned short to) {
    static unsigned int serial = 0;
    srand((unsigned int)(time(NULL)) +
          (unsigned int)(getpid()) +
          serial++);
    return (rand() % (to - from)) + from;
}

unsigned short ZkServer::choosePort() {
    while(1)
    {
        unsigned short port = getPort();
        int testSock;
        testSock = socket(AF_INET, SOCK_STREAM, 0);
        if (testSock == -1) {
            continue;
        }

        struct sockaddr_in my_addr;
        my_addr.sin_family = AF_INET;
        my_addr.sin_port = htons(port);
        my_addr.sin_addr.s_addr = INADDR_ANY;
        bzero(&(my_addr.sin_zero),sizeof(my_addr.sin_zero));

        if(bind(testSock,(struct sockaddr *)&my_addr,sizeof(struct sockaddr))==-1) {
//            std::cout << "bind " << port << " failed " << strerror(errno) << std::endl;
            close(testSock);
            continue;
        }
        close(testSock);
        return port;
    }
    return 2181;
}

END_HA3_NAMESPACE(util);

