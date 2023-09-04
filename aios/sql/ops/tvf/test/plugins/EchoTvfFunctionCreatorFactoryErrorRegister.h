#pragma once

// #include "sql/ops/tvf/TvfFuncCreatorFactory.h"

namespace sql {

class EchoTvfFunctionCreatorFactoryErrorRegister {
public:
    EchoTvfFunctionCreatorFactoryErrorRegister();
    ~EchoTvfFunctionCreatorFactoryErrorRegister();

    // public:
    //     bool registerTvfFunctions() override;
};

// typedef std::shared_ptr<TvfFuncCreatorFactory> TvfFuncCreatorFactoryPtr;
} // namespace sql
