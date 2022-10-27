#ifndef ISEARCH_SIMPLEMATCHDATA_H
#define ISEARCH_SIMPLEMATCHDATA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/Trait.h>

BEGIN_HA3_NAMESPACE(rank);

class SimpleMatchData
{
public:
    SimpleMatchData();
    ~SimpleMatchData();
private:
    SimpleMatchData(const SimpleMatchData &);
    SimpleMatchData& operator=(const SimpleMatchData &);
public:
    // return need bit count.
    static uint32_t sizeOf(uint32_t termCount) {
        if (termCount == 0) {
            return 0;
        }
        return ((termCount - 1) / (sizeof(uint32_t) * 8) + 1) * sizeof(uint32_t);
    }
private:
    static const uint32_t SLOT_TAIL = 0x1f;
public:
    bool isMatch(uint32_t idx) const {
        uint32_t s = idx >> 5;
        uint32_t f = idx & SLOT_TAIL;
        return _matchBuffer[s] & (1 << f);
    }
public:
    void setMatch(uint32_t idx, bool isMatch) {
        uint32_t s = idx >> 5;
        uint32_t f = idx & SLOT_TAIL;
        if (isMatch) {
            _matchBuffer[s] |= (1 << f);
        } else {
            _matchBuffer[s] &= ~(1 << f);
        }
    }
private:
    uint32_t _matchBuffer[1];
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SimpleMatchData);

END_HA3_NAMESPACE(rank);

NOT_SUPPORT_CLONE_TYPE(isearch::rank::SimpleMatchData);
NOT_SUPPORT_SERIZLIE_TYPE(isearch::rank::SimpleMatchData);
DECLARE_NO_NEED_DESTRUCT_TYPE(isearch::rank::SimpleMatchData);

#endif //ISEARCH_SIMPLEMATCHDATA_H
