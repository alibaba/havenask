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
#ifndef ISEARCH_BS_DOCPROCESSORCHAINCONFIG_H
#define ISEARCH_BS_DOCPROCESSORCHAINCONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/ProcessorInfo.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class DocProcessorChainConfig : public autil::legacy::Jsonizable
{
public:
    DocProcessorChainConfig();
    ~DocProcessorChainConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;

public:
    std::vector<std::string> clusterNames;
    plugin::ModuleInfos moduleInfos;
    ProcessorInfos processorInfos;
    ProcessorInfos subProcessorInfos;
    uint32_t indexDocSerializeVersion;
    bool tolerateFormatError;
    bool useRawDocAsDoc;
    bool serializeRawDoc;
    std::string chainName;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocProcessorChainConfig);
typedef std::vector<DocProcessorChainConfig> DocProcessorChainConfigVec;
BS_TYPEDEF_PTR(DocProcessorChainConfigVec);

}} // namespace build_service::config

#endif // ISEARCH_BS_DOCPROCESSORCHAINCONFIG_H
