#include "build_service/util/test/FakeSwiftClient.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "build_service/util/test/FakeSwiftWriter.h"
#include "swift/client/SwiftClientConfig.h"
#include "swift/network/SwiftAdminAdapter.h"

using namespace std;
using namespace swift::client;
using namespace swift::network;

namespace build_service { namespace util {
BS_LOG_SETUP(util, FakeSwiftClient);

FakeSwiftClient::FakeSwiftClient(bool hasReader, bool hasWriter) : _hasReader(hasReader), _hasWriter(hasWriter) {}

FakeSwiftClient::~FakeSwiftClient() {}

SwiftReader* FakeSwiftClient::createReader(const string& readerConfigStr, swift::protocol::ErrorInfo* errorCode)
{
    if (_hasReader) {
        return createSwiftReader(SwiftAdminAdapterPtr(), SwiftRpcChannelManagerPtr());
    }
    BS_LOG(ERROR, "FakeSwiftClient can not create reader, _hasReader = false");
    return nullptr;
}

SwiftWriter* FakeSwiftClient::createWriter(const std::string& writerConfigStr, swift::protocol::ErrorInfo* errorCode)
{
    if (_hasWriter) {
        return new FakeSwiftWriter(writerConfigStr);
    }
    return nullptr;
}

}} // namespace build_service::util
