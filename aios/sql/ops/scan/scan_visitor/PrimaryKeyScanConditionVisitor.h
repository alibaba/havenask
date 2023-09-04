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
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/condition/ConditionVisitor.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace sql {
class AndCondition;
class LeafCondition;
class NotCondition;
class OrCondition;
} // namespace sql

namespace sql {

class PrimaryKeyScanConditionVisitor : public ConditionVisitor {
public:
    PrimaryKeyScanConditionVisitor(const std::string &keyFieldName,
                                   const std::string &keyIndexName);
    ~PrimaryKeyScanConditionVisitor();

public:
    void visitAndCondition(AndCondition *condition) override;
    void visitOrCondition(OrCondition *condition) override;
    void visitNotCondition(NotCondition *condition) override;
    void visitLeafCondition(LeafCondition *condition) override;
    bool stealHasQuery() {
        bool tmp = _hasQuery;
        _hasQuery = false;
        return tmp;
    }
    bool needFilter() {
        return _needFilter;
    }
    const std::vector<std::string> &getRawKeyVec() const {
        return _rawKeyVec;
    }
    std::vector<std::string> stealRawKeyVec() {
        return std::move(_rawKeyVec);
    }

protected:
    bool parseUdf(const autil::SimpleValue &condition);
    bool parseKey(const autil::SimpleValue &condition);
    bool parseIn(const autil::SimpleValue &condition);
    bool parseEqual(const autil::SimpleValue &left, const autil::SimpleValue &right);
    bool doParseEqual(const autil::SimpleValue &left, const autil::SimpleValue &right);
    bool tryAddKeyValue(const autil::SimpleValue &attr, const autil::SimpleValue &value);

protected:
    suez::turing::TableInfoPtr _tableInfo;
    std::string _keyFieldName;
    std::string _keyIndexName;
    std::vector<std::string> _rawKeyVec;
    std::set<std::string> _rawKeySet;
    bool _hasQuery;
    bool _needFilter;
};
typedef std::shared_ptr<PrimaryKeyScanConditionVisitor> PrimaryKeyScanConditionVisitorPtr;
typedef PrimaryKeyScanConditionVisitor SummaryScanConditionVisitor;
typedef PrimaryKeyScanConditionVisitor KVScanConditionVisitor;
typedef PrimaryKeyScanConditionVisitor KKVScanConditionVisitor;
typedef std::shared_ptr<SummaryScanConditionVisitor> SummaryScanConditionVisitorPtr;
typedef std::shared_ptr<KVScanConditionVisitor> KVScanConditionVisitorPtr;
typedef std::shared_ptr<KKVScanConditionVisitor> KKVScanConditionVisitorPtr;
/**
class SummaryScanConditionVisitor : public PrimaryKeyScanConditionVisitor
{
public:
    SummaryScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName)
        : PrimaryKeyScanConditionVisitor(keyFieldName, keyIndexName)
    {}
    ~SummaryScanConditionVisitor()
    {}
};
typedef std::shared_ptr<SummaryScanConditionVisitor> SummaryScanConditionVisitorPtr;

class KVScanConditionVisitor : public PrimaryKeyScanConditionVisitor
{
public:
    KVScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName)
        : PrimaryKeyScanConditionVisitor(keyFieldName, keyIndexName)
    {}
    ~KVScanConditionVisitor()
    {}
};
typedef std::shared_ptr<KVScanConditionVisitor> KVScanConditionVisitorPtr;

class KKVScanConditionVisitor : public PrimaryKeyScanConditionVisitor
{
public:
    KKVScanConditionVisitor(const std::string& keyFieldName, const std::string& keyIndexName)
        : PrimaryKeyScanConditionVisitor(keyFieldName, keyIndexName)
    {}
    ~KKVScanConditionVisitor()
    {}
};
typedef std::shared_ptr<KKVScanConditionVisitor> KKVScanConditionVisitorPtr;*/

} // namespace sql
