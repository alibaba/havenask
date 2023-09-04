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
#ifndef CARBON_DIFF_SERIALIZER_H
#define CARBON_DIFF_SERIALIZER_H

#include "master/Serializer.h"
#include "common/common.h"
#include "carbon/Log.h"

BEGIN_CARBON_NAMESPACE(master);

class DiffSerializer
{
public:
    DiffSerializer(SerializerPtr impl) : _impl(impl) {
    }

    bool write(const std::string& content) {
        {
            autil::ScopedLock lock(_mutex);
            if (_last == content) return true;
            _last = content;
        }
        return _impl->write(content);
    }

    bool read(std::string& content) const {
        return _impl->read(content);
    }

    bool checkExist(bool& exist) { return _impl->checkExist(exist); }

private:
    std::string _last;
    SerializerPtr _impl;
    mutable autil::ThreadMutex _mutex;
};

CARBON_TYPEDEF_PTR(DiffSerializer);

END_CARBON_NAMESPACE(master);

#endif // CARBON_DIFF_SERIALIZER_H
