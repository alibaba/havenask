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
#ifndef __BEEPER_TYPEDEF_H
#define __BEEPER_TYPEDEF_H

#include <memory>
#include <sys/time.h>
#include "autil/legacy/jsonizable.h"

namespace beeper {

enum EventLevel
{
    EL_UNKNOWN = 0
};

inline const char* LevelToString(EventLevel level)
{
    return "unknown";
}

inline EventLevel LevelFromString(const std::string& level)
{
    return EL_UNKNOWN;
}


////////////////////////////////////////////////////////////////////////////
class EventTags
{
public:
    typedef std::map<std::string, std::string> TagMap;
    typedef TagMap::const_iterator TagMapIter;

public:
    EventTags(const TagMap& _kvMap) {}
    EventTags() {}
    ~EventTags() {}
    void AddTag(const std::string &k, const std::string &v)
    {

    }
    void MergeTags(EventTags* tags) const
    {
    }
    const std::string& FindTag(const std::string &k) const {
        static std::string emptyStr;
        return emptyStr;
    }
    void DelTag(const std::string& k) { }
    size_t Size() const { return 0; }
};
typedef std::shared_ptr<EventTags> EventTagsPtr;


////////////////////////////////////////////////////////////////////////////
class Event
{
public:
    Event(const std::string& collecterId, const std::string& _msg,
          const EventTagsPtr& _tags, int64_t _ts)
    {}
};

typedef std::shared_ptr<Event> EventPtr;

}

#endif //__BEEPER_BEEPER_H
