#pragma once

#include <string>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"

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
