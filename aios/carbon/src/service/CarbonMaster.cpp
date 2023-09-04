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
#include "service/CarbonMaster.h"
#include "carbon/Log.h"
#include "common/PathUtil.h"
#include "master/SerializerCreator.h"
#include "hippo/ProtoWrapper.h"
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
//#include <hsfcpp/hsf.h>

using namespace std;
using namespace hippo;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
//using namespace hsf;

USE_CARBON_NAMESPACE(common);
USE_CARBON_NAMESPACE(master);
BEGIN_CARBON_NAMESPACE(service);

CARBON_LOG_SETUP(master, CarbonMaster);

CarbonMaster::CarbonMaster() {
    _carbonDriverPtr.reset(new CarbonDriver(CARBON_MODE_BINARY));
    _carbonMasterServiceImplPtr.reset(new CarbonMasterServiceImpl(_carbonDriverPtr));
    _carbonOpsServiceImplPtr.reset(new CarbonOpsServiceImpl(_carbonDriverPtr));
}

CarbonMaster::~CarbonMaster() {
}

void CarbonMaster::doStop() {
    stopRPCServer();
    _carbonMasterServiceImplPtr.reset();
    _carbonOpsServiceImplPtr.reset();
    _carbonDriverPtr->stop();
    _carbonDriverPtr.reset();
    //hsf_core::destroy();
    shutdownLeaderElector();
}

void CarbonMaster::doInitOptions() {
    OptionParser &optionParser = getOptionParser();
    optionParser.addOption("-z", "--zk", "zkPath",
                            OptionParser::OPT_STRING, true);
    optionParser.addOption("", "--hippo", "hippoZkPath",
                            OptionParser::OPT_STRING, true);
    optionParser.addOption("-a", "--appId", "appId",
                            OptionParser::OPT_STRING, true);
    optionParser.addOption("", "--amonitorPort", "amonitorPort"
                           , DEFAULT_AMONITOR_AGENT_PORT);
    optionParser.addOption("-b", "--backupRoot", "backupRoot",
                            OptionParser::OPT_STRING, false);
}

void CarbonMaster::doExtractOptions() {
    OptionParser &optionParser = getOptionParser();
    optionParser.getOptionValue("backupRoot", _backupRoot);
    optionParser.getOptionValue("zkPath", _zkPath);
    optionParser.getOptionValue("hippoZkPath", _hippoZkPath);
    optionParser.getOptionValue("appId", _appId);
    optionParser.getOptionValue("amonitorPort", _amonitorPort);
}

bool CarbonMaster::doInit() {
    if (!registerService(_carbonMasterServiceImplPtr.get())) {
        CARBON_LOG(ERROR, "register simple master service failed!");
        return false;
    }
    if (!registerService(_carbonOpsServiceImplPtr.get())) {
        CARBON_LOG(ERROR, "register ops service failed!");
        return false;
    }
    return initHTTPRPCServer();
}

bool CarbonMaster::doStart() {
    _masterAddr = getIp() + ":" + StringUtil::toString(getPort());

    if (!initLeaderElector(_zkPath)) {
        CARBON_LOG(ERROR, "init master leader checker failed!");
        return false;
    }
    
    return true;
}

bool CarbonMaster::doPrepareToRun() {
    if (!isLeader()) {
        return false;
    }
    
    if (!start()) {
        stop();
        return false;
    }
    return true;
}

void CarbonMaster::doIdle() {
    if (!isLeader()) {
        CARBON_LOG(ERROR, "no longer be leader, it will stop ...");
        stop();
    }
}

bool CarbonMaster::start() {
    bool isNewStart = false;
    if (!checkIsNewStart(isNewStart)) {
        CARBON_LOG(ERROR, "check is new start failed");
        return false;
    }
    if (!_carbonDriverPtr->init(_appId, _hippoZkPath, _zkPath,
                                _backupRoot, getLeaderElector(), isNewStart,
                                _amonitorPort, false))
    {
        CARBON_LOG(ERROR, "init carbon driver failed");
        return false;
    }

    if (isNewStart) {
        deleteNewStartTag();
    }
    
    if (!_carbonDriverPtr->start()) {
        CARBON_LOG(ERROR, "start carbon driver failed! it will stop...");
        stop();
        return false;
    }
    return true;
}

bool CarbonMaster::checkIsNewStart(bool &isNewStart) {
    isNewStart = false;
    string newStartFilePath = PathUtil::joinPath(getZkPath(),
            CARBON_NEW_START_TAG);
    
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(newStartFilePath);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        CARBON_LOG(ERROR, "check simple master new start failed. "
               "new start tag file: %s, error:%s",
               newStartFilePath.c_str(),
               fslib::fs::FileSystem::getErrorString(ec).c_str());
        return false; 
    } else if (ec == fslib::EC_TRUE) {
        isNewStart = true;
    }
    
    return true;
}

bool CarbonMaster::deleteNewStartTag() {
    string newStartFilePath = PathUtil::joinPath(getZkPath(),
            CARBON_NEW_START_TAG);
    if (fslib::fs::FileSystem::remove(newStartFilePath) == fslib::EC_OK){
        CARBON_LOG(INFO, "delete newStart tag file [%s] success!",
               newStartFilePath.c_str());
        return true;
    } else {
        CARBON_LOG(ERROR, "delete newStart tag file [%s] failed!",
               newStartFilePath.c_str());
        return false;
    }
}

END_CARBON_NAMESPACE(master);

