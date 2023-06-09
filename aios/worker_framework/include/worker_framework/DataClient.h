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
#pragma once

#include <list>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "worker_framework/DataItem.h"

namespace worker_framework {

class DeployClient;
typedef std::shared_ptr<DeployClient> DeployClientPtr;
class DeployDataItem;
typedef std::shared_ptr<DeployDataItem> DeployDataItemPtr;

struct DataTaskInfo {
    std::string srcBaseUri;
    std::string dstDir;
    DataStatus status;
};

class DataClient {
public:
    DataClient();
    DataClient(int32_t port);
    virtual ~DataClient();

private:
    DataClient(const DataClient &);
    DataClient &operator=(const DataClient &);

public:
    virtual bool init();
    /*
      get the whole remote dir from srcBaseUri to dstDir
     */
    virtual DataItemPtr
    getData(const std::string &srcBaseUri, const std::string &dstDir, const DataOption &option = DataOption());

    /*
      get the srcFilePathes in srcBaseUri to dstDir
    */
    virtual DataItemPtr getData(const std::string &srcBaseUri,
                                const std::vector<std::string> &srcFilePathes,
                                const std::string &dstDir,
                                const DataOption &option = DataOption());

    /*
      get the srcFileMetas in srcBaseUri to dstDir
    */
    virtual DataItemPtr getData(const std::string &srcBaseUri,
                                const std::vector<DataFileMeta> &srcFileMetas,
                                const std::string &dstDir,
                                const DataOption &option = DataOption(false));

    bool queryDataTaskInfoWithDstDirPrefix(const std::string &dstDirPrefix, std::vector<DataTaskInfo> &dataTaskInfos);
    /*
      remove the data, if immediately = false, the data will move to
      HIPPO_SLAVE_TRASHDIR and will be removed from disk later, or the data
      will be removed immediately.
      NOTE: the deploying data must be canceled first.
    */
    bool removeData(const std::string &dataPath, bool immediately = false);
    // not virtual for compatibility
    bool init(const std::string &slaveSpec);

private:
    void watch();
    bool getEnv(const std::string &name, std::string *value);
    bool initPort();
    bool startThread();

public:
    // public for test
    bool runInHippoContainer();
    bool redressPath(std::string *path);

private:
    friend class DataClientTest;
    DeployClientPtr _deployClientPtr;
    std::list<DeployDataItemPtr> _deployDataItems;
    autil::ThreadMutex _lock;
    autil::LoopThreadPtr _watchThreadPtr;
    int32_t _port;
};

typedef std::shared_ptr<DataClient> DataClientPtr;

}; // namespace worker_framework
