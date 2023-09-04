#pragma once

#include <stdint.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/MountInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace matchdoc {

class MatchDocTestHelper {
public:
    MatchDocTestHelper() {}
    ~MatchDocTestHelper() {}

public:
    static MatchDocAllocatorPtr createAllocator(const std::string &despStr,
                                                const std::shared_ptr<autil::mem_pool::Pool> &pool,
                                                const MountInfoPtr &mountInfo = MountInfoPtr());

    static MatchDoc createMatchDoc(const MatchDocAllocatorPtr &allocator, const std::string &valueStr);

    static bool
    checkDocValue(const MatchDocAllocatorPtr &allocator, const MatchDoc &matchDoc, const std::string &expectedValue);

    static char *createMountData(const std::string &despStr, autil::mem_pool::Pool *pool);

    static MountInfoPtr createMountInfo(const std::string &despStr);

    static bool setSerializeLevel(const MatchDocAllocatorPtr &allocator, const std::string &levelStr);

    static bool checkSerializeLevel(const MatchDocAllocatorPtr &allocator,
                                    uint8_t serializeLevel,
                                    const std::string &expectLevelStr);

    static bool checkDeserializedDocValue(const MatchDocAllocatorPtr &allocator,
                                          const MatchDoc &matchDoc,
                                          uint8_t serializeLevel,
                                          const std::string &expectedValue);

    static MatchDocAllocator::ReferenceAliasMapPtr createReferenceAliasMap(const std::string &aliasRelation);
};
} // namespace matchdoc
