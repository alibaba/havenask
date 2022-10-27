#include "build_service/reader/EmptyFilePattern.h"

using namespace std;

namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, EmptyFilePattern);

EmptyFilePattern::EmptyFilePattern()
    : FilePatternBase("")
{
}

EmptyFilePattern::~EmptyFilePattern() {
}

CollectResults EmptyFilePattern::calculateHashIds(const CollectResults &results) const {
    CollectResults ret;
    uint32_t fileCount = static_cast<uint32_t>(results.size());

    for (size_t i = 0; i < results.size(); ++i) {
        uint32_t fileId = static_cast<uint32_t>(i);
        uint16_t rangeId = getRangeId(fileId, fileCount);

        CollectResult now = results[i];
        now._rangeId = rangeId;
        ret.push_back(now);
    }

    return ret;
}

}
}
