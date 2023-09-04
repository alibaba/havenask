#pragma once

// #include "sql/ops/tvf/TvfFuncCreatorFactory.h"

namespace sql {

class EchoTvfFunctionCreatorFactoryError {
public:
    EchoTvfFunctionCreatorFactoryError();
    ~EchoTvfFunctionCreatorFactoryError();

    // public:
    //     bool registerTvfFunctions() override;
};

// typedef std::shared_ptr<TvfFuncCreatorFactory> TvfFuncCreatorFactoryPtr;
} // namespace sql
