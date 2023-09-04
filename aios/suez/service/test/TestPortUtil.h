#pragma once

#include <netinet/in.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

class TestPortUtil {
public:
    static bool SelectUnusedPorts(std::vector<int> &ports, int32_t num = 1) {
        std::vector<int> sockets;
        for (auto idx = 0; idx < num; idx++) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                return false;
            }
            struct sockaddr_in serv_addr;
            bzero((char *)&serv_addr, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_addr.s_addr = INADDR_ANY;
            serv_addr.sin_port = 0;
            if (bind(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                return false;
            }
            socklen_t len = sizeof(serv_addr);
            if (getsockname(sock, (struct sockaddr *)&serv_addr, &len) == -1) {
                return false;
            }
            ports.push_back(ntohs(serv_addr.sin_port));
            sockets.push_back(sock);
        }
        for (auto &socket : sockets) {
            if (close(socket) < 0) {
                return false;
            }
        }
        return true;
    }
};
