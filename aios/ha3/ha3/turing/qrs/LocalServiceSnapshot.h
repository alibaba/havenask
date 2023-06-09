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
#include "suez/sdk/RpcServer.h"

#include "ha3/isearch.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "ha3/turing/qrs/QrsSqlBiz.h"
#include "ha3/turing/qrs/QrsServiceSnapshot.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"

namespace isearch {
namespace turing {

class LocalServiceSnapshot : public QrsServiceSnapshot
{
public:
    LocalServiceSnapshot(bool enableSql);
    ~LocalServiceSnapshot();
private:
    LocalServiceSnapshot(const LocalServiceSnapshot &);
    LocalServiceSnapshot& operator=(const LocalServiceSnapshot &);
protected:
    suez::BizMetas addExtraBizMetas(const suez::BizMetas &currentBizMetas) const override;
    suez::turing::BizBasePtr createBizBase(const std::string &bizName,
            const suez::BizMeta &bizMeta) override;
    suez::turing::SearchContext *doCreateContext(
            const suez::turing::SearchContextArgs &args,
            const suez::turing::GraphRequest *request,
            suez::turing::GraphResponse *response) override;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LocalServiceSnapshot> LocalServiceSnapshotPtr;

} // namespace turing
} // namespace isearch
