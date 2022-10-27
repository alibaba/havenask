#ifndef ISEARCH_RANKSIZECONVERTER_H
#define ISEARCH_RANKSIZECONVERTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(config);

class RankSizeConverter
{
public:
    RankSizeConverter();
    ~RankSizeConverter();
private:
    RankSizeConverter(const RankSizeConverter &);
    RankSizeConverter& operator=(const RankSizeConverter &);
public:
    static uint32_t convertToRankSize(const std::string &rankSize);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RankSizeConverter);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_RANKSIZECONVERTER_H
