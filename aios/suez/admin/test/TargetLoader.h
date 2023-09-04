#pragma once

#include <string>

#include "suez/sdk/JsonNodeRef.h"

namespace suez {

class TargetLoader {
public:
    static bool loadFromFile(const std::string &fileName, JsonNodeRef::JsonMap &target);
};

} // namespace suez
