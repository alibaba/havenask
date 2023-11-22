#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/SwiftWriter.h"
#include "swift/protocol/ErrCode.pb.h"

namespace build_service { namespace util {

class FakeSwiftWriter : public swift::client::SwiftWriter
{
public:
    FakeSwiftWriter();
    FakeSwiftWriter(const std::string& writerConfigStr) { _writerConfigStr = writerConfigStr; }
    ~FakeSwiftWriter();

private:
    FakeSwiftWriter(const FakeSwiftWriter&);
    FakeSwiftWriter& operator=(const FakeSwiftWriter&);

public:
    swift::protocol::ErrorCode write(swift::client::MessageInfo& msgInfo) override
    {
        return swift::protocol::ERROR_NONE;
    }
    int64_t getLastRefreshTime(uint32_t pid) const override { return 0; }
    int64_t getCommittedCheckpointId() const override { return 0; }

    std::pair<int32_t, uint16_t> getPartitionIdByHashStr(const std::string& zkPath, const std::string& topicName,
                                                         const std::string& hashStr) override
    {
        return std::make_pair<int32_t, uint16_t>(0, 0);
    }
    std::pair<int32_t, uint16_t> getPartitionIdByHashStr(const std::string& hashStr) override
    {
        return std::make_pair<int32_t, uint16_t>(0, 0);
    }
    void getWriterMetrics(const std::string& zkPath, const std::string& topicName,
                          swift::client::WriterMetrics& writerMetric) override
    {
    }

    bool isSyncWriter() const override { return true; }
    bool clearBuffer(int64_t& cpBeg, int64_t& cpEnd) override { return true; }
    void setForceSend(bool forceSend) override {}
    swift::common::SchemaWriter* getSchemaWriter() override { return nullptr; }
    bool isFinished() override { return true; }
    bool isAllSent() override { return true; }
    void setCommittedCallBack(const std::function<void(const std::vector<std::pair<int64_t, int64_t>>&)>& func) override
    {
    }
    void setErrorCallBack(const std::function<void(const swift::protocol::ErrorCode&)>& func) override {}

    const std::string& getWriterConfig() { return _writerConfigStr; }

private:
    std::string _writerConfigStr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeSwiftWriter);

}} // namespace build_service::util
