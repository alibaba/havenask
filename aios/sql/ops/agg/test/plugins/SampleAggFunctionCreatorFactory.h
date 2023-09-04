#pragma once

// #include "sql/ops/agg/AggFuncCreatorFactory.h"

namespace sql {

class SampleAggFunctionCreatorFactory {
public:
    SampleAggFunctionCreatorFactory();
    ~SampleAggFunctionCreatorFactory();

    // public:
    //     bool registerAggFunctions(autil::InterfaceManager *manager) override;

    // private:
    //     bool registerAggFunctionInfos() override;
};

// typedef std::shared_ptr<AggFuncCreatorFactory> AggFuncCreatorFactoryPtr;
} // namespace sql
