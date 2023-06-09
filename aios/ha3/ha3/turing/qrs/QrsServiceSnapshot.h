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

#include "iquan/jni/Iquan.h"
#include "suez/turing/search/Biz.h"

#include "ha3/isearch.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/turing/qrs/SqlQueryCache.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "ha3/turing/qrs/QrsSqlBiz.h"
#include "ha3/turing/common/Ha3BizMeta.h"
#include "ha3/turing/common/Ha3ServiceSnapshot.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"

namespace isearch {
namespace turing {

class QrsServiceSnapshot : public Ha3ServiceSnapshot
{
public:
    QrsServiceSnapshot(bool enableSql);
    ~QrsServiceSnapshot();
private:
    QrsServiceSnapshot(const QrsServiceSnapshot &);
    QrsServiceSnapshot& operator=(const QrsServiceSnapshot &);
protected:
    suez::turing::BizBasePtr createBizBase(const std::string &bizName,
            const suez::BizMeta &bizMeta) override;
    bool doInit(const suez::turing::SnapshotBaseOptions &options,
                suez::turing::ServiceSnapshotBase *oldSnapshot) override;
    void calculateBizMetas(const suez::BizMetas &currentBizMetas,
                           ServiceSnapshotBase *oldSnapshot,
                           suez::BizMetas &toUpdate,
                           suez::BizMetas &toKeep) override;
public:
    std::string getBizName(const std::string &bizName,
                           const suez::BizMeta &bizMeta) const override;
    QrsBizPtr getFirstQrsBiz();
    QrsSqlBizPtr getFirstQrsSqlBiz();
    proto::PartitionID getPid() const{
        return _pid;
    }
    std::string getAddress() const{
        return _address;
    }
    iquan::Iquan *getSqlClient() { return _sqlClientPtr.get(); }
    iquan::IquanPtr getSqlClientPtr() { return _sqlClientPtr; }

    SqlQueryCache *getSqlQueryCache() {
        return &_sqlQueryCache;
    }

protected:
    virtual suez::BizMetas addExtraBizMetas(const suez::BizMetas &currentBizMetas) const;
private:
    std::vector<std::string> getTopoBizNames(QrsBiz *qrsBiz) const;
    std::vector<std::string> getExternalPublishBizNames();
    void calTopoInfo(multi_call::TopoInfoBuilder &infoBuilder) override;
    void initAddress();
    bool initPid();
    bool getRange(uint32_t partCount, uint32_t partId, autil::PartitionRange &range);
    bool doWarmup() override;
    bool initSqlClient(const Ha3BizMeta &ha3BizMeta);
    bool enableSql() const;
    bool hasQrsSqlBiz();

private:
    mutable bool _enableSql;
    proto::PartitionID _pid;
    std::string _address;
    std::shared_ptr<iquan::Iquan> _sqlClientPtr;
    SqlQueryCache _sqlQueryCache;
private:
    AUTIL_LOG_DECLARE();

private:
    static const std::string EXTRA_PUBLISH_BIZ_NAME;
};

typedef std::shared_ptr<QrsServiceSnapshot> QrsServiceSnapshotPtr;

} // namespace turing
} // namespace isearch
