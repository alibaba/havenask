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
#ifndef NAVI_RESOURCEINITCONTEXT_H
#define NAVI_RESOURCEINITCONTEXT_H

#include "navi/engine/ResourceMap.h"

namespace navi {

class ResourceInitContext
{
public:
    ResourceInitContext(const std::string &configPath,
                        const std::string &bizName,
                        NaviPartId partCount,
                        NaviPartId partId,
                        const ResourceMap &resourceMap);
    ~ResourceInitContext();
private:
    ResourceInitContext(const ResourceInitContext &);
    ResourceInitContext &operator=(const ResourceInitContext &);
public:
    Resource *getDependResource(const std::string &name) const;
    template <typename T>
    T *getDependResource(const std::string &name) const {
        return dynamic_cast<T *>(getDependResource(name));
    }
    const ResourceMap &getDependResourceMap() const;
    const std::string &getBizName() const {
        return _bizName;
    }
    NaviPartId getPartCount() const {
        return _partCount;
    }
    NaviPartId getPartId() const {
        return _partId;
    }
    const std::string &getConfigPath() const;
private:
    const std::string &_configPath;
    const std::string &_bizName;
    NaviPartId _partCount;
    NaviPartId _partId;
    const ResourceMap &_resourceMap;

};

}

#endif //NAVI_RESOURCEINITCONTEXT_H
