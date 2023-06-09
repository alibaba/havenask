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
#ifndef _ALOG_STRINGUTIL_H
#define _ALOG_STRINGUTIL_H

#include <string>
#include <vector>
#include <climits>

namespace alog {

class StringUtil {
public:

    /**
      *@biref Returns a string identical to the given string but without leading
      *@n or trailing HTABs or spaces.
    **/
    static std::string trim(const std::string& s);

    /**
      *@brief splits a string into a vector of string segments based on the
      *@n given delimiter.
      *@param v The vector in which the segments will be stored. The vector
      *@n will be emptied before storing the segments
      *@param s The string to split into segments.
      *@param delimiter The delimiter character
      *@param maxSegments the maximum number of segments. Upon return
      *@n v.size() <= maxSegments.  The string is scanned from left to right
      *@n so v[maxSegments - 1] may contain a string containing the delimiter
      *@n character.
    *@return The actual number of segments (limited by maxSegments).
    **/
    static unsigned int split(std::vector<std::string>& v,
                              const std::string& s, char delimiter,
                              unsigned int maxSegments = INT_MAX);
    /**
       @brief splits a string into string segments based on the given delimiter
       @n and assigns the segments through an output_iterator.
       @param output The output_iterator through which to assign the string
       @n segments. Typically this will be a back_insertion_iterator.
       @param s The string to split into segments.
       @param delimiter The delimiter character
       @param maxSegments The maximum number of segments.
       @return The actual number of segments (limited by maxSegments).
    **/
    template<typename T> static unsigned int split(T& output,
            const std::string& s, char delimiter,
            unsigned int maxSegments = INT_MAX)
    {
        std::string::size_type left = 0;
        unsigned int i;
        for (i = 1; i < maxSegments; i++)
        {
            std::string::size_type right = s.find(delimiter, left);
            if (right == std::string::npos)
            {
                break;
            }
            *output++ = s.substr(left, right - left);
            left = right + 1;
        }
        std::string lastSubStr = s.substr(left);
        if (lastSubStr != "")
            *output++ = lastSubStr;
        return i;
    }

};
}

#endif // _ALOG_STRINGUTIL_H

