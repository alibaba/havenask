#pragma once

#include "autil/Log.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlibv2::table {

class RangeQueryParser
{
public:
    RangeQueryParser();
    ~RangeQueryParser();

public:
    static bool ParseQueryStr(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedConfig,
                              const std::string& range, int64_t& leftTimestamp, int64_t& rightTimestamp);

    static bool ParseTimestamp(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedConfig,
                               const std::string& str, int64_t& value);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
