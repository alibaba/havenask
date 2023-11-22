#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/query/function_executor.h"
#include "indexlib/document/query/function_executor_factory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class ExampleFunctionExecutor : public FunctionExecutor
{
public:
    ExampleFunctionExecutor();
    ~ExampleFunctionExecutor();

public:
    bool DoInit(const util::KeyValueMap& kvMap) override
    {
        QueryFunction::FunctionParam param;
        if (!mFunction.GetFunctionParam(0, param)) {
            return false;
        }
        mFieldName = param.second;
        if (!mFunction.GetFunctionParam(1, param)) {
            return false;
        }
        mValue = param.second;
        return true;
    }

    std::string ExecuteFunction(const RawDocumentPtr& rawDoc, bool& hasError) override
    {
        if (!rawDoc->exist(mFieldName)) {
            hasError = true;
            return std::string("");
        }

        if (mFunction.GetFunctionName() == "plus") {
            hasError = false;
            int v1 = autil::StringUtil::fromString<int64_t>(rawDoc->getField(mFieldName));
            int v2 = autil::StringUtil::fromString<int64_t>(mValue);
            return autil::StringUtil::toString(v1 + v2);
        }

        if (mFunction.GetFunctionName() == "append") {
            hasError = false;
            return rawDoc->getField(mFieldName) + mValue;
        }
        hasError = true;
        return std::string("");
    }

private:
    std::string mFieldName;
    std::string mValue;

private:
    IE_LOG_DECLARE();
};

class ExampleFunctionExecutorFactory : public FunctionExecutorFactory
{
public:
    ExampleFunctionExecutorFactory() {}
    virtual ~ExampleFunctionExecutorFactory() {}

public:
    void destroy() override { delete this; }

public:
    FunctionExecutor* CreateFunctionExecutor(const QueryFunction& function, const util::KeyValueMap& kvMap) override;

public:
    const static std::string FACTORY_NAME;
};

DEFINE_SHARED_PTR(ExampleFunctionExecutor);
}} // namespace indexlib::document
