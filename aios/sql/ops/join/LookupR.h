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

#include "navi/engine/Resource.h"
#include "sql/ops/join/HashJoinMapR.h"
#include "sql/ops/join/JoinBaseParamR.h"
#include "sql/ops/join/JoinInfoCollectorR.h"
#include "sql/ops/join/LookupJoinBatch.h"
#include "table/Table.h"

namespace sql {

class JoinBase;
class LookupJoinKernel;
class IndexInfo;
struct StreamQuery;
struct LookupJoinBatch;

class LookupR : public navi::Resource {
public:
    LookupR();
    ~LookupR();
    LookupR(const LookupR &) = delete;
    LookupR &operator=(const LookupR &) = delete;

public:
    virtual void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    virtual navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    virtual std::shared_ptr<StreamQuery> genFinalStreamQuery(const LookupJoinBatch &batch);
    bool joinTable(const LookupJoinBatch &batch,
                   const table::TablePtr &streamOutput,
                   table::TablePtr &outputTable);
    bool finishJoin(const table::TablePtr &table, size_t offset, table::TablePtr &outputTable);
    bool finishJoinEnd(const table::TablePtr &table, size_t offset, table::TablePtr &outputTable);
    size_t getJoinedCount() const {
        return _hasJoinedCount;
    }
    bool isLeftTableIndexed() const {
        return _leftTableIndexed;
    }

protected:
    bool doHashJoin(const LookupJoinBatch &batch, const table::TablePtr &streamTable);

private:
    bool createHashMap(const table::TablePtr &table, size_t offset, size_t count);
    bool genStreamKeys(const LookupJoinBatch &batch, std::vector<std::string> &pks);
    virtual bool doJoinTable(const LookupJoinBatch &batch, const table::TablePtr &streamOutput) {
        return doHashJoin(batch, streamOutput);
    }

private:
    static bool getPkTriggerField(const std::vector<std::string> &inputFields,
                                  const std::vector<std::string> &joinFields,
                                  const std::map<std::string, sql::IndexInfo> &indexInfoMap,
                                  std::string &triggerField);
    static bool genStreamQueryTerm(const table::TablePtr &input,
                                   table::Row row,
                                   const std::string &inputField,
                                   std::vector<std::string> &termVec);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

protected:
    RESOURCE_DEPEND_ON(JoinInfoCollectorR, _joinInfoR);
    RESOURCE_DEPEND_ON(JoinBaseParamR, _joinParamR);
    RESOURCE_DEPEND_ON(HashJoinMapR, _hashJoinMapR);
    std::vector<std::string> *_lookupColumns = nullptr;
    std::vector<std::string> *_joinColumns = nullptr;
    std::map<std::string, sql::IndexInfo> *_lookupIndexInfos = nullptr;
    std::map<std::string, sql::IndexInfo> _leftIndexInfos;
    std::map<std::string, sql::IndexInfo> _rightIndexInfos;
    bool _leftTableIndexed = false;
    size_t _hasJoinedCount = 0;
};

} // namespace sql
