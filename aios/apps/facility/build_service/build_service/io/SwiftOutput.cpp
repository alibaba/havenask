/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/io/SwiftOutput.h"

using namespace std;
using namespace autil::legacy;
using namespace build_service::util;

using namespace swift::protocol;
using namespace swift::client;
using namespace swift::common;

namespace build_service { namespace io {
BS_LOG_SETUP(io, SwiftOutput);

SwiftOutput::SwiftOutput(const config::TaskOutputConfig& outputConfig)
    : Output(outputConfig)
    , _swiftClientCreator(new SwiftClientCreator())
{
}

SwiftOutput::~SwiftOutput()
{
    _swiftWriter.reset();
    _swiftClientCreator.reset();
}

bool SwiftOutput::init(const KeyValueMap& initParams)
{
    mergeParams(initParams);
    const auto& param = _outputConfig.getParameters();
    try {
        const string& zkRoot = param.at("zk_root");
        auto client = _swiftClientCreator->createSwiftClient(zkRoot, "");
        if (!client) {
            BS_LOG(ERROR, "create client failed");
            return false;
        }
        auto topicName = param.at("topic_name");
        auto writerConfig =
            getValueFromKeyValueMap(param, "writer_config", "functionChain=hashId2partId;mode=async|safe");
        // TODO: check writer config mode should be async|safe, otherwise commit is not safe
        string writerConfigStr = string("topicName=") + topicName + ";" + writerConfig;
        swift::protocol::ErrorInfo err;
        auto swiftWriter = client->createWriter(writerConfigStr, &err);
        if (!swiftWriter || err.errcode() != swift::protocol::ERROR_NONE) {
            BS_LOG(ERROR, "create swift writer failed, error [%s]", err.errmsg().c_str());
            return false;
        }
        _swiftWriter.reset(swiftWriter);
    } catch (const std::out_of_range& e) {
        BS_LOG(ERROR, "swift raw document output init failed, param error: [%s]", e.what());
        return false;
    }
    return true;
}

bool SwiftOutput::write(Any& any)
{
    try {
        auto msg = AnyCast<MessageInfo>(&any);
        return write(*msg);
    } catch (const BadAnyCast& e) {
        BS_LOG(ERROR, "BadAnyCast: %s", e.what());
        return false;
    }
}

bool SwiftOutput::write(MessageInfo& msg)
{
    auto ec = _swiftWriter->write(msg);
    while (ec != ERROR_NONE) {
        BS_LOG(ERROR, "writer swift error: [%s]", ErrorCode_Name(ec).c_str());
        ec = _swiftWriter->write(msg);
    }
    return true;
}

bool SwiftOutput::commit()
{
    static constexpr int64_t WAIT_FINISH_TIME_US = 1800 * 1000 * 1000; // 30min
    auto ec = _swiftWriter->waitFinished(WAIT_FINISH_TIME_US);
    if (ec != ERROR_NONE) {
        BS_LOG(ERROR, "commit swift error: [%s]", ErrorCode_Name(ec).c_str());
        return false;
    }
    return true;
}
}} // namespace build_service::io
