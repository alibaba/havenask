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
#ifndef ISEARCH_BS_INDEXRECLAIMCONFIGMAKER_H
#define ISEARCH_BS_INDEXRECLAIMCONFIGMAKER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"

namespace build_service { namespace task_base {

class IndexReclaimConfigMaker
{
public:
    IndexReclaimConfigMaker(const std::string& clusterName);
    ~IndexReclaimConfigMaker();

private:
    IndexReclaimConfigMaker(const IndexReclaimConfigMaker&);
    IndexReclaimConfigMaker& operator=(const IndexReclaimConfigMaker&);

public:
    void parseOneMessage(const std::string& msgData);
    std::string makeReclaimParam();

private:
    std::string _clusterName;
    int64_t _msgCount;
    int64_t _matchCount;
    int64_t _invalidMsgCount;

    typedef std::vector<indexlib::merger::IndexReclaimerParamPtr> ReclaimParamVec;
    std::map<std::string, ReclaimParamVec> _singleTermParams;
    ReclaimParamVec _andOperateParams;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexReclaimConfigMaker);

}} // namespace build_service::task_base

#endif // ISEARCH_BS_INDEXRECLAIMCONFIGMAKER_H
