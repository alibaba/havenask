#ifndef ISEARCH_BS_FAKESWIFTCLIENT_H
#define ISEARCH_BS_FAKESWIFTCLIENT_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftClient.h"

namespace build_service { namespace util {

class FakeSwiftClient : public swift::client::SwiftClient
{
public:
    FakeSwiftClient(bool hasReader, bool hasWriter);
    ~FakeSwiftClient();

private:
    FakeSwiftClient(const FakeSwiftClient&);
    FakeSwiftClient& operator=(const FakeSwiftClient&);

public:
    virtual swift::client::SwiftReader* createReader(const std::string& readerConfigStr,
                                                     swift::protocol::ErrorInfo* errorCode) override;
    virtual swift::client::SwiftWriter* createWriter(const std::string& writerConfigStr,
                                                     swift::protocol::ErrorInfo* errorCode) override;

private:
    bool _hasReader;
    bool _hasWriter;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeSwiftClient);

}} // namespace build_service::util

#endif // ISEARCH_BS_FAKESWIFTCLIENT_H
