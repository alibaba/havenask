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
#ifndef ISEARCH_BS_PROCESSORPARAMPARSER_H
#define ISEARCH_BS_PROCESSORPARAMPARSER_H

#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/ProcessorInput.h"
#include "build_service/common/ProcessorOutput.h"
#include "build_service/common_define.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

struct ProcessorConfigInfo {
    ProcessorConfigInfo() : isLastStage(false) {}
    std::string configPath;
    std::string configName;
    std::string stage;
    std::string preStage;
    bool isLastStage;
};

class ProcessorParamParser
{
public:
    ProcessorParamParser();
    ~ProcessorParamParser();

private:
    ProcessorParamParser(const ProcessorParamParser&);
    ProcessorParamParser& operator=(const ProcessorParamParser&);

public:
    bool parse(const KeyValueMap& kvMap, const TaskResourceManagerPtr& resMgr);
    const ProcessorInput& getInput() { return _processorInput; }
    const std::vector<std::string>& getClusterNames() { return _clusterNames; }
    // const std::string& getConfigPath() { return _configInfo.configPath; }
    const ProcessorConfigInfo& getConfigInfo() { return _configInfo; }
    const common::ProcessorOutput& getOutput() { return _processorOutput; }

    // todo: delete when graph support json str--yijie
    static bool parseIOConfig(const std::string& input, config::TaskInputConfig* inputConfig);

private:
    bool parseOutput(const KeyValueMap& kvMap, const config::ResourceReaderPtr& resourceReader,
                     const TaskResourceManagerPtr& resMgr);

private:
    ProcessorInput _processorInput;
    common::ProcessorOutput _processorOutput;
    std::vector<std::string> _clusterNames;
    // std::string _configPath;
    ProcessorConfigInfo _configInfo;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorParamParser);

}} // namespace build_service::admin

#endif // ISEARCH_BS_PROCESSORPARAMPARSER_H
