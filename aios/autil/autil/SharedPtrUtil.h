/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>
#include <stdint.h>
#include <unistd.h>

namespace autil {

class SharedPtrUtil {
private:
    SharedPtrUtil();
    ~SharedPtrUtil();

private:
    SharedPtrUtil(const SharedPtrUtil &);
    SharedPtrUtil &operator=(const SharedPtrUtil &);

public:
    template <class T>
    static bool waitUseCount(const std::shared_ptr<T> &ptr, uint32_t expectUseCount = 1, int64_t timeout = -1) {
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
};

} // namespace autil
