#pragma once
#include <string>

#include "autil/legacy/RapidJsonCommon.h"

namespace autil {
namespace legacy {

class JsonTestUtil {
public:
    static void checkJsonStringEqual(const std::string &expected, const std::string &actual);

private:
    static bool deepSortKeys(SimpleValue &value);
    static void prettyDocument(SimpleDocument &document, std::string &result);
};

} // namespace legacy
} // namespace autil
