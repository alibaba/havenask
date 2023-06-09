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
#include "suez/service/LeaderRouter.h"

#include "autil/Log.h"
#include "suez/sdk/RemoteTableWriterRequestGenerator.h"
#include "suez/service/GetSchemaVersionGenerator.h"
#include "suez/service/UpdateSchemaGenerator.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, LeaderRouter);

LeaderRouter::LeaderRouter() {}

LeaderRouter::~LeaderRouter() {}

bool LeaderRouter::init(const std::string &zoneName) {
    _zoneName = zoneName;
    return true;
}

#define ERROR_THEN_RETURN(error_code, msg)                                                                             \
    {                                                                                                                  \
        ErrorInfo *errInfo = response->mutable_errorinfo();                                                            \
        errInfo->set_errorcode(error_code);                                                                            \
        errInfo->set_errormsg(msg);                                                                                    \
        return;                                                                                                        \
    }

void LeaderRouter::updateSchema(const multi_call::SearchServicePtr &searchService,
                                const UpdateSchemaRequest *request,
                                UpdateSchemaResponse *response) {
    if (request->partids().empty()) {
        ERROR_THEN_RETURN(DS_ERROR_GET_SCHEMA_VERSION, "empty partIds, at least 1 id is required")
    }
    auto bizName = GeneratorDef::getTableWriteBizName(request->zonename(), request->tablename());
    AUTIL_LOG(INFO, "update schema for bizname: %s", bizName.c_str());
    auto generator = std::make_shared<UpdateSchemaGenerator>(bizName, request);
    multi_call::SearchParam searchParam;
    searchParam.generatorVec.push_back(generator);
    multi_call::ReplyPtr reply;
    searchService->search(searchParam, reply);
    fillUpdateResponse(reply, response);
}

void LeaderRouter::fillUpdateResponse(const multi_call::ReplyPtr &reply, UpdateSchemaResponse *response) {
    size_t lackCount = 0;
    auto responseVec = reply->getResponses(lackCount);
    bool hasFailed = false;
    for (const auto &gigResponse : responseVec) {
        auto partId = gigResponse->getPartId();
        if (gigResponse->isFailed()) {
            AUTIL_LOG(ERROR,
                      "update schema failed, partId [%d], ec [%s]",
                      partId,
                      multi_call::translateErrorCode(gigResponse->getErrorCode()));
            hasFailed = true;
            response->mutable_failpartids()->Add(partId);
        }
    }
    if (lackCount > 0 || hasFailed) {
        response->mutable_errorinfo()->set_errorcode(DS_ERROR_UPDATE_SCHEMA);
        response->mutable_errorinfo()->set_errormsg("some part has error");
    }
}

void LeaderRouter::getSchemaVersion(const multi_call::SearchServicePtr &searchService,
                                    const GetSchemaVersionRequest *request,
                                    GetSchemaVersionResponse *response) {
    if (request->partids().empty()) {
        ERROR_THEN_RETURN(DS_ERROR_GET_SCHEMA_VERSION, "empty partIds, at least 1 id is required")
    }
    auto bizName = GeneratorDef::getTableWriteBizName(request->zonename(), request->tablename());
    auto generator = std::make_shared<GetSchemaVersionGenerator>(bizName, request);
    multi_call::SearchParam searchParam;
    searchParam.generatorVec.push_back(generator);
    multi_call::ReplyPtr reply;
    searchService->search(searchParam, reply);
    fillGetSchemaVersionResponse(reply, response);
}

void LeaderRouter::fillGetSchemaVersionResponse(const multi_call::ReplyPtr &reply, GetSchemaVersionResponse *response) {
    size_t lackCount = 0;
    auto responseVec = reply->getResponses(lackCount);
    bool hasFailed = false;
    for (const auto &gigResponse : responseVec) {
        auto partId = gigResponse->getPartId();
        if (gigResponse->isFailed()) {
            AUTIL_LOG(ERROR,
                      "get schema version failed, partId [%d], ec [%s]",
                      partId,
                      multi_call::translateErrorCode(gigResponse->getErrorCode()));
            hasFailed = true;
            response->mutable_failpartids()->Add(partId);
            continue;
        }
        auto getSchemaResponse = dynamic_cast<GigGetSchemaVersionResponse *>(gigResponse.get());
        if (!getSchemaResponse) {
            AUTIL_LOG(ERROR, "get schema version failed, invalid response type, partId [%d]", partId);
            hasFailed = true;
            response->mutable_failpartids()->Add(partId);
            continue;
        }
        auto pbResponse = dynamic_cast<suez::GetSchemaVersionResponse *>(getSchemaResponse->getMessage());
        if (!pbResponse) {
            AUTIL_LOG(ERROR, "get schema version failed, null pb response, partId [%d]", partId);
            hasFailed = true;
            response->mutable_failpartids()->Add(partId);
            continue;
        }
        auto tableSchemaVersion = response->add_schemaversions();
        tableSchemaVersion->set_partid(partId);
        tableSchemaVersion->set_currentversionid(pbResponse->schemaversions(0).currentversionid());
    }
    if (lackCount > 0 || hasFailed) {
        response->mutable_errorinfo()->set_errorcode(DS_ERROR_UPDATE_SCHEMA);
        response->mutable_errorinfo()->set_errormsg("some part has error");
    }
}

} // namespace suez
