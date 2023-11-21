#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace test {

class RangeQueryParser
{
public:
    RangeQueryParser();
    ~RangeQueryParser();

public:
    static bool ParseQueryStr(const config::IndexConfigPtr& indexConfig, const std::string& range,
                              int64_t& leftTimestamp, int64_t& rightTimestamp);

    static bool ParseTimestamp(const config::IndexConfigPtr& indexConfig, const std::string& str, int64_t& value);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeQueryParser);
}} // namespace indexlib::test
