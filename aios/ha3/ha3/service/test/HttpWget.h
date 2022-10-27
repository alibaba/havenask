#ifndef ISEARCH_HTTPWGET_H
#define ISEARCH_HTTPWGET_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <anet/anet.h>

BEGIN_HA3_NAMESPACE(service);

class HttpWget
{
public:
    HttpWget(const std::string &host, uint32_t port);
    ~HttpWget();
private:
    HttpWget(const HttpWget &);
    HttpWget& operator = (const HttpWget &);
public:
    anet::HTTPPacket* getReply(const std::string &path,
                               bool isKeepAlive = true);
    std::string getBody(const std::string &url);
    std::string getBody(const std::string &path,
                        int &statusCode, bool isKeepAlive = true);
    anet::Connection* getConn() {
        return _connection;
    }
private:
    anet::Transport *_transport;
    anet::Connection *_connection;
    anet::HTTPPacketFactory *_factory;
    anet::HTTPStreamer *_streamer;
    char _spec[200];
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HttpWget);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_HTTPWGET_H
