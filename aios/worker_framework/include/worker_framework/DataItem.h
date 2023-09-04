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

#include "autil/Lock.h"
#include "worker_framework/DataOption.h"

namespace worker_framework {

enum DataStatus {
    DS_UNKNOWN,
    DS_DEPLOYING,
    DS_FINISHED,
    DS_FAILED,
    DS_STOPPED,
    DS_RETRYING,
    DS_DEST,    // dst addr fail, sometimes we will exit with this situation
    DS_CANCELED // used internal
};

class DataFileMeta {
public:
    DataFileMeta() {
        modifyTime = -1; // indexlib already use -1 as default value
        length = -1;
        isDir = false;
        isCheckExist = true;
    }

public:
    std::string path;
    uint64_t modifyTime;
    int64_t length;
    bool isDir;
    bool isCheckExist;
};

class DataItem {
public:
    DataItem(const std::string &srcBaseUri, const std::string &dstDir, const DataOption &dataOption);
    virtual ~DataItem();

private:
    DataItem(const DataItem &);
    DataItem &operator=(const DataItem &);

public:
    DataStatus getStatus();

    // for test
    void setStatus(DataStatus status) { _status = status; };

    std::string getDstDir() { return _dstDir; }

    /*
      cancel the DEPLOYING data task, it will become STOPPED or FAILED
     */
    void cancel();

protected:
    virtual void doCancel(){};

protected:
    DataStatus _status;
    std::string _srcBaseUri;
    std::string _dstDir;
    std::string _errorMsg;
    int32_t _retryCount;
    DataOption _dataOption;
    autil::ThreadMutex _lock;
};

typedef std::shared_ptr<DataItem> DataItemPtr;

}; // namespace worker_framework
