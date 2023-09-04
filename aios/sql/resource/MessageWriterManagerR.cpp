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
#include "sql/resource/MessageWriterManagerR.h"

#include <engine/NaviConfigContext.h>

#include "autil/legacy/legacy_jsonizable.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GigClientR.h"
#include "sql/resource/MessageWriterManager.h"
#include "sql/resource/SwiftMessageWriterManager.h"
#include "sql/resource/TableMessageWriterManager.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string MessageWriterManagerR::RESOURCE_ID = "MESSAGE_WRITER_MANAGER_R";

MessageWriterManagerR::MessageWriterManagerR() {}

MessageWriterManagerR::~MessageWriterManagerR() {}

void MessageWriterManagerR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool MessageWriterManagerR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "swift_writer_config", _swiftWriterConfig, _swiftWriterConfig);
    NAVI_JSONIZE(ctx, "table_writer_config", _tableWriterConfig, _tableWriterConfig);
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
    if (!_tableWriterConfig.empty()) {
        _messageWriterManager.reset(new TableMessageWriterManager(_gigClientR->getSearchService()));
        std::string configStr = FastToJsonString(_tableWriterConfig, true);
        if (!_messageWriterManager->init(configStr)) {
            NAVI_KERNEL_LOG(
                ERROR, "init table writer manager failed, config is [%s].", configStr.c_str());
            _messageWriterManager.reset();
            return false;
        }
        return true;
    }
    if (!_swiftWriterConfig.empty()) {
        _messageWriterManager.reset(new SwiftMessageWriterManager());
        std::string configStr = FastToJsonString(_swiftWriterConfig, true);
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

} // namespace sql
