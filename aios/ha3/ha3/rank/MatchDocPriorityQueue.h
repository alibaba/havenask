#ifndef ISEARCH_SCOREDOCPRIORITYQUEUE_H
#define ISEARCH_SCOREDOCPRIORITYQUEUE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <assert.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/Comparator.h>

BEGIN_HA3_NAMESPACE(rank);

class MatchDocPriorityQueue
{
public:
    MatchDocPriorityQueue(uint32_t size, autil::mem_pool::Pool *pool,
                          const Comparator *cmp);
    virtual ~MatchDocPriorityQueue();
public:
    enum PUSH_RETURN_CODE {
        ITEM_ACCEPTED = 1,
        ITEM_DENIED,
        ITEM_REPLACED,
    };

    void setComparator(const Comparator *cmp) {
        assert(cmp);
        _queueCmp = cmp;
    }

    const Comparator* getComparator() const {
        return _queueCmp;
    }

    matchdoc::MatchDoc pop();
    uint32_t count() const {
        return _count;
    }
    uint32_t size() const {
        return _size;
    }

    matchdoc::MatchDoc top() const {
        if (_count > 0) {
            return _items[1];
        }
        return matchdoc::INVALID_MATCHDOC;
    }

    void quickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) {
        assert(count <= _size);
        memcpy(_items + 1, matchDocs, sizeof(matchdoc::MatchDoc) * count);
        _count = count;
        for (int32_t i = _count / 2; i >= 1; --i) {
            adjustDown(i);
        }
    }

    void adjustUp(uint32_t idx);
    void adjustDown(uint32_t idx);

    void reset();
    matchdoc::MatchDoc *getAllMatchDocs() {
        return _items + 1;
    }
    matchdoc::MatchDoc &item(uint32_t idx) {
        return _items[idx];
    }
public:
    PUSH_RETURN_CODE push(matchdoc::MatchDoc item,
                          matchdoc::MatchDoc *retItem);
    bool isFull() const { return _size == _count; }
    virtual void swap(uint32_t a, uint32_t b);
protected:
    uint32_t _size;
    uint32_t _count;
    const Comparator *_queueCmp;
    matchdoc::MatchDoc *_items;
private:
    friend class MatchDocPriorityQueueTest;
    HA3_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////

inline MatchDocPriorityQueue::PUSH_RETURN_CODE
MatchDocPriorityQueue::push(matchdoc::MatchDoc item, matchdoc::MatchDoc *retItem) {
    if (isFull()) {
        if (!_queueCmp->compare(_items[1], item)) {
            *retItem = item;
            return ITEM_DENIED;
        } else {
            *retItem = _items[1];
            _items[1] = item;
            swap(1, 1);
            adjustDown(1);
            return ITEM_REPLACED;
        }
    }
    _count++;
    _items[_count] = item;
    swap(_count, _count);
    adjustUp(_count);
    return ITEM_ACCEPTED;
}

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_SCOREDOCPRIORITYQUEUE_H
