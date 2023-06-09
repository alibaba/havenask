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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "expression/framework/AtomicAttributeExpressionCreatorBase.h"
namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class Segment;
}

namespace expression {
class AttributeExpressionPool;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {
class AtomicAttributeExpressionBase;

class AtomicExpressionCreator : public expression::AtomicAttributeExpressionCreatorBase
{
public:
    AtomicExpressionCreator(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                            const std::shared_ptr<config::TabletSchema>& schema,
                            expression::AttributeExpressionPool* exprPool, autil::mem_pool::Pool* pool);
    ~AtomicExpressionCreator();

public:
    expression::AttributeExpression* createAtomicExpr(const std::string& name, const std::string& tableName) override;
    void endRequest() override;
    bool beginRequest(std::vector<expression::AttributeExpression*>& allExprInSession,
                      expression::SessionResource* resource) override;
    size_t GetCreatedExpressionCount() const;
    AtomicAttributeExpressionBase* GetCreatedExpression(size_t index) const;

private:
    std::vector<AtomicAttributeExpressionBase*> _uniqCreatedExpressions;
    std::vector<AtomicAttributeExpressionBase*> _createdExpressions;
    std::vector<std::shared_ptr<framework::Segment>> _segments;
    std::shared_ptr<config::TabletSchema> _schema;
    expression::AttributeExpressionPool* _exprPool;
    autil::mem_pool::Pool* _pool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
