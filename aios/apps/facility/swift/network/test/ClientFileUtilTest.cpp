#include "swift/network/ClientFileUtil.h"

#include <iostream>
#include <netinet/in.h>
#include <stdlib.h>
#include <string>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "fstream" // IWYU pragma: keep
#include "sstream" // IWYU pragma: keep
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace network {

class ClientFileUtilTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void writeFile(const string &path, const string &content) {
        ofstream out(path.c_str());
        out.write(content.c_str(), content.size());
        out.close();
    }
};

void ClientFileUtilTest::setUp() {}

void ClientFileUtilTest::tearDown() {}

TEST_F(ClientFileUtilTest, testReadLocalFile) {
    string existFile = GET_TEST_DATA_PATH() + "/read_file.txt";
    ClientFileUtil fileUtil;
    string content;
    ASSERT_TRUE(fileUtil.readLocalFile(existFile, content));
    ASSERT_EQ("abc aa\n====\naabc\n", content);
    string content1;
    ASSERT_TRUE(fileUtil.readFile(existFile, content1));
    ASSERT_EQ(content, content1);

    ASSERT_FALSE(fileUtil.readLocalFile("/tmp/1211111", content));
}

/*TEST_F(ClientFileUtilTest, testReadZfsFile) {
    heartbeat::ZkServer zkServer;
    std::ostringstream oss;
    oss << "127.0.0.1:" << zkServer.port();
    string zkRoot = "zfs://" + oss.str() + "/";
    {
        string path = zkRoot+"123";
        string writeContent(1024, 'a');
        writeFile(path, writeContent);
        ClientFileUtil fileUtil;
        string content;
        ASSERT_TRUE(fileUtil.readZfsFile(path, content));
        ASSERT_EQ(writeContent, content);
        string content1;
        ASSERT_TRUE(fileUtil.readFile(path, content1));
        ASSERT_EQ(content, content1);

        ASSERT_FALSE(fileUtil.readZfsFile(zkRoot+"11/", content));
        ASSERT_FALSE(fileUtil.readZfsFile(zkRoot+"112", content));
    }
    {
        string path = zkRoot+"12345";
        string writeContent(10240, 'a');
        writeFile(path, writeContent);
        ClientFileUtil fileUtil;
        string content;
        ASSERT_TRUE(fileUtil.readZfsFile(path, content));
        ASSERT_EQ(writeContent, content);
        string content1;
        ASSERT_TRUE(fileUtil.readFile(path, content1));
        ASSERT_EQ(content, content1);
    }
    {
        string path = zkRoot+"123456";
        string writeContent(10241, 'a');
        writeFile(path, writeContent);
        ClientFileUtil fileUtil;
        string content;
        ASSERT_TRUE(fileUtil.readZfsFile(path, content));
        ASSERT_EQ(writeContent, content);
        string content1;
        ASSERT_TRUE(fileUtil.readFile(path, content1));
        ASSERT_EQ(content, content1);
    }
}
*/
class SimpleHttpServer {
public:
    SimpleHttpServer(string path) {
        vector<int> ports;
        if (!selectUnusedPorts(ports, 1)) {
            _port = "33245";
        } else {
            _port = to_string(ports[0]);
        }
        _path = path;
        start();
    }

    ~SimpleHttpServer() { stop(); }

public:
    void start() {
        string cmd = "cd " + _path + "; python -m SimpleHTTPServer " + _port + " &";
        cout << cmd << endl;
        (void)system(cmd.c_str());
    }
    void stop() {
        string cmd = "ps uxwww | grep SimpleHTTPServer| grep " + _port +
                     " | grep -v grep | awk -F' ' '{print $2}' | xargs kill -9";
        cout << cmd << endl;
        (void)system(cmd.c_str());
        sleep(1);
    }
    string getPort() { return _port; }

private:
    static bool selectUnusedPorts(std::vector<int> &ports, int32_t num = 1) {
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

private:
    string _port;
    string _path;
};

TEST_F(ClientFileUtilTest, testReadHttpFile) {
    SimpleHttpServer httpServer(GET_TEST_DATA_PATH());
    ClientFileUtil fileUtil;
    string path = "http://127.0.0.1:" + httpServer.getPort() + "/read_file.txt";
    string path1 = "http://127.0.0.1:" + httpServer.getPort() + "/read_file1.txt";
    sleep(1);
    string content;
    ASSERT_TRUE(fileUtil.readHttpFile(path, content));
    ASSERT_EQ("abc aa\n====\naabc\n", content);
    string content1;
    ASSERT_TRUE(fileUtil.readFile(path, content1));
    ASSERT_EQ(content1, content);

    ASSERT_TRUE(fileUtil.readHttpFile(path1, content));
    ASSERT_TRUE(content.find("Error code 404") != string::npos);
}

TEST_F(ClientFileUtilTest, testReadHttpFileServerFail) {
    ClientFileUtil fileUtil;
    string path = "http://101.199.199.199:54699/read_file.txt";
    string content;
    ASSERT_FALSE(fileUtil.readHttpFile(path, content));
}

TEST_F(ClientFileUtilTest, testReadHttpFileServerFail2) {
    ClientFileUtil fileUtil;
    string path = "http://fs-proxyxxx.vip.tbsite.net:54699/read_file.txt";
    string content;
    ASSERT_FALSE(fileUtil.readHttpFile(path, content));
}

TEST_F(ClientFileUtilTest, testGetFsType) {
    ClientFileUtil fileUtil;
    string type;
    ASSERT_EQ("http", fileUtil.getFsType("http://aaa"));
    ASSERT_EQ("zfs", fileUtil.getFsType("zfs://aaa"));
    ASSERT_EQ("local", fileUtil.getFsType("/aaa"));
    ASSERT_EQ("", fileUtil.getFsType(""));
}

TEST_F(ClientFileUtilTest, testParsePath) {
    ClientFileUtil fileUtil;
    string httpStr = "http://fs-proxy.vip.tbsite.net:3066/swift_st3_online";
    string server, path;
    ASSERT_TRUE(fileUtil.parsePath("http://", httpStr, server, path));
    ASSERT_EQ("fs-proxy.vip.tbsite.net:3066", server);
    ASSERT_EQ("/swift_st3_online", path);
    string zfsStr = "zfs://search-zk-swift-st3.vip.tbsite.net:12182/swift/swift_hippo_st3_mainse";
    ASSERT_FALSE(fileUtil.parsePath("zfs://", httpStr, server, path));
    ASSERT_TRUE(fileUtil.parsePath("zfs://", zfsStr, server, path));
    ASSERT_EQ("search-zk-swift-st3.vip.tbsite.net:12182", server);
    ASSERT_EQ("/swift/swift_hippo_st3_mainse", path);
}

}; // namespace network
}; // namespace swift
