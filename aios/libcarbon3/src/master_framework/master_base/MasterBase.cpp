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
#include "common/Log.h"
#include "master_framework/MasterBase.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace hippo;
using namespace autil;
BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

MASTER_FRAMEWORK_LOG_SETUP(master_base, MasterBase);

MasterBase::MasterBase() {
}

MasterBase::~MasterBase() {
}

void MasterBase::doInitOptions() {
    OptionParser &optionParser = getOptionParser();
    optionParser.addOption("-z", "--zk", "zkPath",
                            OptionParser::OPT_STRING, true);
    optionParser.addOption("", "--hippo", "hippoZkPath",
                            OptionParser::OPT_STRING, true);
    optionParser.addOption("-a", "--appId", "appId",
                            OptionParser::OPT_STRING, true);
}

void MasterBase::doExtractOptions() {
    OptionParser &optionParser = getOptionParser();
    optionParser.getOptionValue("zkPath", _zkPath);
    optionParser.getOptionValue("hippoZkPath", _hippoZkPath);
    optionParser.getOptionValue("appId", _appId);
}

bool MasterBase::doInit() {
    return true;
}

bool MasterBase::doStart() {
    _masterAddr = getIp() + ":" + StringUtil::toString(getPort());

    if (!initLeaderElector(_zkPath)) {
        MF_LOG(ERROR, "init master leader checker failed!");
        return false;
    }
    
    return true;
}

bool MasterBase::doPrepareToRun() {
    if (!isLeader()) {
        return false;
    }

    return true;
}

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

