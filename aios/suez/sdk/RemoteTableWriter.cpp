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
#include "suez/sdk/RemoteTableWriter.h"

#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "autil/Log.h"
#include "autil/result/Errors.h"
#include "multi_call/interface/QuerySession.h"
#include "multi_call/interface/Reply.h"
#include "suez/sdk/RemoteTableWriterClosure.h"
#include "suez/sdk/RemoteTableWriterParam.h"
#include "suez/sdk/RemoteTableWriterRequestGenerator.h"

using namespace std;
using namespace autil;
using namespace autil::result;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RemoteTableWriter);

RemoteTableWriter::RemoteTableWriter(const std::string &zoneName,
                                     multi_call::SearchService *searchService,
                                     bool allowFollowWrite)
    : _zoneName(zoneName), _searchService(searchService), _allowFollowWrite(allowFollowWrite) {}

RemoteTableWriter::~RemoteTableWriter() {}

void RemoteTableWriter::write(const std::string &tableName, const std::string &format, MessageWriteParam writeParam) {
    RemoteTableWriterParam param;
    param.bizName = GeneratorDef::getTableWriteBizName(_zoneName, tableName);
    param.tableName = tableName;
    param.format = format;
    param.docs = std::move(writeParam.docs);
    param.timeoutUs = writeParam.timeoutUs;
    auto generator = std::make_shared<RemoteTableWriterRequestGenerator>(std::move(param));
    if (_allowFollowWrite) {
        generator->setLeaderTags(multi_call::TMT_PREFER);
    } else {
        generator->setLeaderTags(multi_call::TMT_REQUIRE);
    }
    multi_call::SearchParam searchParam;
    searchParam.generatorVec.push_back(generator);
    auto *closure = new RemoteTableWriterClosure(std::move(writeParam.cb));
    searchParam.closure = closure;
    if (writeParam.querySession) {
        writeParam.querySession->call(searchParam, closure->getReplyPtr());
    } else {
        _searchService->search(searchParam, closure->getReplyPtr());
    }
}

} // namespace suez
