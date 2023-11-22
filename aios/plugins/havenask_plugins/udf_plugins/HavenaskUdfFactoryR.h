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
#include "havenask_plugins/udf_plugins/HavenaskUdfFactory.h"
#include "suez/turing/expression/function/FunctionFactoryBaseR.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"


BEGIN_HAVENASK_UDF_NAMESPACE(factory)

class HavenaskUdfFactoryR : public suez::turing::FunctionFactoryBaseR {
public:
    HavenaskUdfFactoryR();
    ~HavenaskUdfFactoryR();
private:
    HavenaskUdfFactoryR(const HavenaskUdfFactoryR &);
    HavenaskUdfFactoryR& operator=(const HavenaskUdfFactoryR &);
public:
    void def(navi::ResourceDefBuilder &builder) const override;
    std::shared_ptr<suez::turing::SyntaxExpressionFactory> createFactory() override {
        return std::make_shared<HavenaskUdfFactory>();
    }
    
public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();
};

END_HAVENASK_UDF_NAMESPACE(factory)
