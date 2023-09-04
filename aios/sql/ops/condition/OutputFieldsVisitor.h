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

#include "autil/legacy/RapidJsonCommon.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/condition/ExprVisitor.h"

namespace sql {

class OutputFieldsVisitor : public ExprVisitor {
public:
    OutputFieldsVisitor();
    ~OutputFieldsVisitor();

public:
    auto &getUsedFieldsItemSet() {
        return _usedFieldsItemSet;
    }
    auto &getUsedFieldsColumnSet() {
        return _usedFieldsColumnSet;
    }

protected:
    void visitCastUdf(const autil::SimpleValue &value) override;
    void visitMultiCastUdf(const autil::SimpleValue &value) override;
    void visitNormalUdf(const autil::SimpleValue &value) override;
    void visitOtherOp(const autil::SimpleValue &value) override;
    void visitItemOp(const autil::SimpleValue &value) override;
    void visitColumn(const autil::SimpleValue &value) override;
    void visitInt64(const autil::SimpleValue &value) override {}
    void visitDouble(const autil::SimpleValue &value) override {}
    void visitString(const autil::SimpleValue &value) override {}

private:
    void visitParams(const autil::SimpleValue &value);

private:
    // select tags['host'], tags['mem'], max_field['docker.cpu'], min_field[xxx]
    std::set<std::pair<std::string, std::string>> _usedFieldsItemSet;
    // select a, b
    std::set<std::string> _usedFieldsColumnSet;
};

typedef std::shared_ptr<OutputFieldsVisitor> OutputFieldsVisitorPtr;
} // namespace sql
