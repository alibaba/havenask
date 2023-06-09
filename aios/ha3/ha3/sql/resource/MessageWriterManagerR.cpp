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
#include "ha3/sql/resource/MessageWriterManagerR.h"

#include "ha3/sql/resource/SqlConfigResource.h"
#include "ha3/sql/resource/SwiftMessageWriterManager.h"
#include "ha3/sql/resource/TableMessageWriterManager.h"
#include "navi/resource/GigClientResource.h"

namespace isearch {
namespace sql {


const std::string MessageWriterManagerR::RESOURCE_ID = "MESSAGE_WRITER_MANAGER_R";

MessageWriterManagerR::MessageWriterManagerR() {
}

MessageWriterManagerR::~MessageWriterManagerR() {
}

void MessageWriterManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID)
        .depend(SqlConfigResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_sqlConfigResource))
        .depend(navi::GIG_CLIENT_RESOURCE_ID, true, BIND_RESOURCE_TO(_gigClientResource));
}

bool MessageWriterManagerR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode MessageWriterManagerR::init(navi::ResourceInitContext &ctx) {
    if (!initMessageWriterManger()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

MessageWriterManager *MessageWriterManagerR::getMessageWriterManager() const {
    return _messageWriterManager.get();
}

bool MessageWriterManagerR::initMessageWriterManger() {
    const auto &tableWriterConfig = _sqlConfigResource->getTableWriterConfig();
    if (!tableWriterConfig.empty()) {
        _messageWriterManager.reset(
            new TableMessageWriterManager(_gigClientResource->getSearchService()));
        std::string configStr = ToJsonString(tableWriterConfig);
        if (!_messageWriterManager->init(configStr)) {
            NAVI_KERNEL_LOG(
                ERROR, "init table writer manager failed, config is [%s].", configStr.c_str());
            _messageWriterManager.reset();
            return false;
        }
        return true;
    }
    const auto &swiftWriterConfig = _sqlConfigResource->getSwiftWriterConfig();
    if (!swiftWriterConfig.empty()) {
        _messageWriterManager.reset(new SwiftMessageWriterManager());
        std::string configStr = ToJsonString(swiftWriterConfig);
        if (!_messageWriterManager->init(configStr)) {
            NAVI_KERNEL_LOG(
                ERROR, "init swift writer manager failed, config is [%s].", configStr.c_str());
            _messageWriterManager.reset();
            return false;
        } else {
            return true;
        }
    }
    return true;
}

REGISTER_RESOURCE(MessageWriterManagerR);

}
}
