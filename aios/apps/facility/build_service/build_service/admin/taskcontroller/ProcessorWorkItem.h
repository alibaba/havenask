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
#ifndef ISEARCH_BS_PROCESSORWORKITEM_H
#define ISEARCH_BS_PROCESSORWORKITEM_H

#include "autil/legacy/jsonizable.h"
#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/common/BrokerTopicKeeper.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class ProcessorWorkItem : public autil::legacy::Jsonizable
{
public:
    ProcessorWorkItem() {}

    ProcessorWorkItem(const ProcessorWorkItem& other);
    // bool operator==(const ProcessorWorkItem &other) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    ProcessorTaskPtr processorTask;
    // BrokerTopicKeeperPtr brokerTopicKeeper;
    // bool isLatest;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorWorkItem);

}} // namespace build_service::admin

#endif // ISEARCH_BS_PROCESSORWORKITEM_H
