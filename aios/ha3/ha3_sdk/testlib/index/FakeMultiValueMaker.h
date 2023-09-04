#pragma once

#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"

namespace indexlib {
namespace index {

class MultiCharMaker {
public:
    MultiCharMaker() {}

    autil::MultiChar makeMultiChar(const std::string &str) {
        autil::MultiChar ret;
        ret.init(autil::MultiValueCreator::createMultiValueBuffer(str.data(), str.size(), &mPool));
        return ret;
    }

public:
    autil::mem_pool::Pool mPool;
};

} // namespace index
} // namespace indexlib
