#ifndef ISEARCH_SCANITERATOR_H
#define ISEARCH_SCANITERATOR_H

#include <matchdoc/MatchDocAllocator.h>
#include <ha3/common/TimeoutTerminator.h>

BEGIN_HA3_NAMESPACE(sql);

class ScanIterator {
public:
    ScanIterator(const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                 common::TimeoutTerminator *timeoutTerminator)
        : _matchDocAllocator(matchDocAllocator)
        , _timeoutTerminator(timeoutTerminator)
        , _totalScanCount(0)
        , _isTimeout(false)
    {
    }

    virtual ~ScanIterator() {
    }

public:
    virtual bool batchSeek(size_t batchSize,
                           std::vector<matchdoc::MatchDoc> &matchDocs) = 0;

    matchdoc::MatchDocAllocatorPtr getMatchDocAllocator() const {
        return _matchDocAllocator;
    }
    virtual uint32_t getTotalScanCount() {
        return _totalScanCount;
    }
    bool isTimeout() const {
        if (_isTimeout) {
            return true;
        } else if (_timeoutTerminator) {
            return _timeoutTerminator->isTimeout();
        } else {
            return false;
        }
    }
protected:
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    common::TimeoutTerminator *_timeoutTerminator;
    uint32_t _totalScanCount;
    bool _isTimeout;

};

HA3_TYPEDEF_PTR(ScanIterator);

END_HA3_NAMESPACE(sql);

#endif
