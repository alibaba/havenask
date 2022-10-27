#ifndef ISEARCH_BS_STREAMQUEUE_H
#define ISEARCH_BS_STREAMQUEUE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/SynchronizedQueue.h>
namespace build_service {
namespace util {

template<typename T>
class StreamQueue
{
public:
    static const uint32_t DEFAULT_QUEUE_SIZE = 1000;
public:
    StreamQueue(uint32_t queueSize = DEFAULT_QUEUE_SIZE) {
        _finish = false;
        _queue.setCapacity(queueSize);
    }
    ~StreamQueue() {}
private:
    StreamQueue(const StreamQueue &);
    StreamQueue& operator=(const StreamQueue &);
public:
    void push(const T &item) {
        _queue.push(item);
    }
    bool pop(T &item) {
        while (true) {
            if (_finish && _queue.isEmpty()) {
                return false;
            }
            if (_queue.tryGetAndPopFront(item)) {
                return true;
            }
            _queue.waitNotEmpty();
        }
    }

    //Attention not thread safe
    bool top(T& item) {
        while (true) {
            if (_finish && _queue.isEmpty()) {
                return false;
            }
            
            if (!_queue.isEmpty()) {
                item = _queue.getFront();
                return true;
            }
            _queue.waitNotEmpty();
        }
    }
    
    uint32_t size()
    {
        return _queue.getSize();
    }

    uint32_t capacity()
    {
        return _queue.getCapacity();
    }

    bool empty()
    {
        return _queue.isEmpty();
    }

    void setFinish() {
        _finish = true;
        _queue.wakeup();
    }
private:
    autil::SynchronizedQueue<T> _queue;
    bool _finish;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_STREAMQUEUE_H
