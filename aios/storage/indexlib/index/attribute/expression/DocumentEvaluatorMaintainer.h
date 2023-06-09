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
#include "indexlib/base/Status.h"
namespace autil::mem_pool {
class Pool;
}
namespace expression {
class AttributeExpressionCreator;
class FunctionInterfaceFactory;
class FunctionInterfaceManager;
} // namespace expression

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {
class DocumentEvaluatorBase;
class TabletSessionResource;
class AtomicExpressionCreator;

class DocumentEvaluatorMaintainer : private autil::NoCopyable
{
public:
    DocumentEvaluatorMaintainer();
    ~DocumentEvaluatorMaintainer();

public:
    Status Init(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                const std::shared_ptr<config::TabletSchema>& schema, const std::vector<std::string>& functionNames);
    std::vector<std::shared_ptr<DocumentEvaluatorBase>> GetAllEvaluators();

private:
    std::shared_ptr<DocumentEvaluatorBase> CreateEvaluator(const std::string& functionName);
    std::shared_ptr<DocumentEvaluatorBase> InnerCreateEvaluator(const std::string& expression);

private:
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::unique_ptr<expression::FunctionInterfaceFactory> _functionInterfaceFactory;
    std::unique_ptr<expression::FunctionInterfaceManager> _functionInterfaceManager;
    std::unique_ptr<expression::AttributeExpressionCreator> _attributeExpressionCreator;
    std::shared_ptr<config::TabletSchema> _schema;
    std::vector<std::shared_ptr<DocumentEvaluatorBase>> _evaluators;
    std::unique_ptr<TabletSessionResource> _sessionResource;
    AtomicExpressionCreator* _atomicExpressionCreator;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
