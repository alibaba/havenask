#ifndef ISEARCH_SHAREDPTRUTIL_H
#define ISEARCH_SHAREDPTRUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <unistd.h>

BEGIN_HA3_NAMESPACE(util);

class SharedPtrUtil
{
public:
    template <class T>
    static bool waitUseCount(const std::tr1::shared_ptr<T> &ptr,
                             uint32_t expectUseCount = 1,
                             int64_t timeout = -1)
    {
        const int64_t interval = 10 * 1000;
        while (ptr.use_count() > expectUseCount) {
            if (timeout != -1) {
                if (timeout <= interval) {
                    return false;
                }
                timeout -= interval;
            }
            usleep(interval);
        }
        return true;
    }
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SharedPtrUtil);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_SHAREDPTRUTIL_H
