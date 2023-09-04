#pragma once

#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeInDocSectionMeta.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"

namespace indexlib {
namespace index {

class FakeMetaMaker {
public:
    static void makeFakeMeta(std::string str, FakeTextIndexReader::DocSectionMap &docSecMetaMap);

private:
    static void parseDocMeta(std::string, FakeInDocSectionMeta::DocSectionMeta &docSecMeta);
    static void parseFieldMeta(std::string, int32_t, FakeTextIndexReader::FieldAndSectionMap &);
    AUTIL_LOG_DECLARE();
};

} // namespace index
} // namespace indexlib
