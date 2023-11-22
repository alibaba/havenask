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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "swift/client/SwiftReader.h"

namespace worker_framework {
class ZkState;

typedef std::shared_ptr<ZkState> ZkStatePtr;
}; // namespace worker_framework

namespace cm_basic {
class ZkWrapper;
}

namespace build_service { namespace task_base {

class BatchReporter;

BS_TYPEDEF_PTR(BatchReporter);

class BatchControlWorker : public autil::legacy::Jsonizable
{
public:
    BatchControlWorker(bool async = true);
    ~BatchControlWorker();

public:
    enum BatchOp { unknown = 0, begin, end };
    class BatchInfo : public autil::legacy::Jsonizable
    {
    public:
        BatchInfo() {}
        BatchInfo(const BatchInfo& other)
            : beginTime(other.beginTime)
            , endTime(other.endTime)
            , batchId(other.batchId)
            , operation(other.operation)
        {
        }
        BatchInfo(BatchOp op, int64_t begin, int64_t end, int32_t batch)
            : beginTime(begin)
            , endTime(end)
            , batchId(batch)
            , operation(op)
        {
        }

    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        bool operator==(const BatchInfo& other) const;

    public:
        int64_t beginTime = -1;
        int64_t endTime = -1;
        int32_t batchId = -1;
        BatchOp operation = BatchOp::unknown;

    public:
        std::string toString() const;
    };

public:
    bool init(Task::TaskInitParam& initParam, beeper::EventTagsPtr tags = beeper::EventTagsPtr());
    virtual bool start(swift::client::SwiftReader* reader, int64_t startTs, const std::string& host, int32_t port);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual bool resetHost(const std::string& host, int32_t port);
    void setDataDescription(const proto::DataDescription& dataDesc);
    void setUseV2Graph(bool useV2);

protected:
    // virtual for test
    virtual void handleError(const std::string& msg);

private:
    void readDocStr();
    void reportBatch();
    void syncCheckpoint();
    bool validate(const BatchInfo& batchInfo) const;
    bool parse(const swift::protocol::Message& msg, BatchInfo& batchInfo) const;
    void process(const BatchInfo& batchInfo);
    void addBatch(BatchInfo batchInfo);
    bool innerReport(const BatchInfo& batchInfo, int64_t globalId, int64_t lastGlobalId);

    virtual bool initZkState(const std::string& zkRoot, const build_service::proto::BuildId& buildId);

private:
    static std::string getHostFromZkPath(const std::string& zkPath);

private:
    mutable autil::RecursiveThreadMutex _lock;
    autil::LoopThreadPtr _readThread;
    autil::LoopThreadPtr _reportThread;
    autil::LoopThreadPtr _syncThread;

    bool _asyncMode; // sync mode for test
    int64_t _startTimestamp;
    int64_t _batchCursor;
    int64_t _locator;
    int64_t _globalId;
    int64_t _finalStepBatchId;
    int64_t _successReportTs;
    int64_t _callGraphFailedTime;
    int64_t _slowBatchStep;
    std::vector<BatchInfo> _batchQueue;

    BatchReporterPtr _reporter;
    swift::client::SwiftReader* _swiftReader;
    beeper::EventTagsPtr _beeperTags;
    proto::DataDescription _dataDesc;
    cm_basic::ZkWrapper* _zkWrapper;
    worker_framework::ZkStatePtr _zkState;

private:
    static const std::string LINE_SEP;
    static const std::string KEY_VALUE_SEP;
    static const std::string KEY_OPERATION;
    static const std::string KEY_OPERATION_BEGIN;
    static const std::string KEY_OPERATION_END;
    static const std::string KEY_BATCH_ID;
    static const int64_t DEFAULT_WAIT_READER_TIME = 500 * 1000; // ms
    static const int64_t DEFAULT_CALL_GRAPH_FAIL_TIME = 60;     // second

    static const std::string BS_CALL_GRAPH_FAILED_TIME;
    static const std::string BS_SLOW_BATCH_STEP;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchControlWorker);

}} // namespace build_service::task_base
