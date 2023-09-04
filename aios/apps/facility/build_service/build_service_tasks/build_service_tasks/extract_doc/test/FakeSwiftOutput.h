#pragma once
#include "build_service/common_define.h"
#include "build_service/io/SwiftOutput.h"

namespace build_service { namespace task_base {

class FakeSwiftOutput : public io::SwiftOutput
{
private:
    using DocFields = std::unordered_map<std::string, std::string>;

public:
    FakeSwiftOutput(const config::TaskOutputConfig& onputConfig);
    ~FakeSwiftOutput();

private:
    FakeSwiftOutput(const FakeSwiftOutput&);
    FakeSwiftOutput& operator=(const FakeSwiftOutput&);

public:
    bool init(const KeyValueMap& params) override { return true; }
    bool write(swift::common::MessageInfo& msg) override
    {
        _msgs.push_back(msg);
        return true;
    }
    bool commit() override { return true; }
    std::vector<swift::common::MessageInfo> getOutputMessages() { return _msgs; }

    bool write(autil::legacy::Any& any) override;

private:
    std::vector<swift::common::MessageInfo> _msgs;

private:
    BS_LOG_DECLARE();
};
BS_TYPEDEF_PTR(FakeSwiftOutput);

}} // namespace build_service::task_base
