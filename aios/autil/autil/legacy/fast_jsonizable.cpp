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

#include "autil/legacy/fast_jsonizable.h"

#include <rapidjson/document.h>
#include <typeinfo>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"

namespace autil {
namespace legacy {

class JsonNumberWriter: public RapidWriter {
public:
    void WriteJsonNumber(const json::JsonNumber &number) {
        Prefix(rapidjson::kNumberType);
        const auto &str = number.AsString();
        for (size_t i = 0; i < str.size(); ++i) {
            os_->Put(str[i]);
        }
    }
    // ATTENTION: do not add payload data
};

void FromRapidValue(Any& t, RapidValue& value) {
#define FROM_BASIC(tps, tp)                     \
    if (value.Is##tps()) {                      \
        tp i = tp();                            \
        FromRapidValue(i, value);               \
        Any o(i);                               \
        t.Swap(o);                              \
        return;                                 \
    }
    FROM_BASIC(Int, int);
    FROM_BASIC(Uint, unsigned);
    FROM_BASIC(Int64, int64_t);
    FROM_BASIC(Uint64, uint64_t);
    FROM_BASIC(Double, double);
    FROM_BASIC(String, std::string);
    FROM_BASIC(Bool, bool);
#undef FROM_BASIC
    if (value.IsObject()) {
        json::JsonMap m;
        for (auto member = value.MemberBegin(); member != value.MemberEnd(); ++member) {
            Any o;
            FromRapidValue(o, member->value);
            m[member->name.GetString()] = o;
        }
        Any o(m);
        t.Swap(o);
        return;
    }
    if (value.IsArray()) {
        json::JsonArray a;
        for (size_t i = 0; i < value.Size(); i++) {
            Any o;
            FromRapidValue(o, value[i]);
            a.push_back(o);
        }
        Any o(a);
        t.Swap(o);
        return;
    }
    if (value.IsNull()) {
        Any o;
        t.Swap(o);
        return;
    }
    AUTIL_LEGACY_THROW(NotSupportException, "RapidValue to Any not support");
}

void serializeToWriter(RapidWriter *writer, const Any& a) {
    decltype(a.GetType()) &type = a.GetType();
    if (type == typeid(void))                               /// void (null)
    {
        writer->Null();
    }
    else if (type == typeid(std::vector<Any>))              /// list
    {
        const json::JsonArray& v = *(AnyCast<json::JsonArray>(&a));
        serializeToWriter(writer, v);
    }
    else if (type == typeid(std::map<std::string, Any>))    /// map
    {
        const json::JsonMap& m = *(AnyCast<json::JsonMap>(&a));
        serializeToWriter(writer, m);
    }
    else if (type == typeid(std::string))               /// string
    {
        serializeToWriter(writer, (*AnyCast<std::string>(&a)));
    } else if (type == typeid(bool))                    /// bool
    {
        serializeToWriter(writer, AnyCast<bool>(a));
    }
    /// different numberic types
#define WRITE_DATA(tp)                                  \
    else if (type == typeid(tp))                        \
    {                                                   \
        serializeToWriter(writer, (AnyCast<tp>(a)));    \
    }
    WRITE_DATA(double)
        WRITE_DATA(float)
        WRITE_DATA(uint16_t)
        WRITE_DATA(int16_t)
        WRITE_DATA(uint32_t)
        WRITE_DATA(int32_t)
        WRITE_DATA(uint64_t)
        WRITE_DATA(int64_t)
#undef WRITE_DATA
#define WRITE_DATA_AS(tp, as)                                           \
        else if (type == typeid(tp))                                    \
        {                                                               \
            serializeToWriter(writer, (static_cast<as>(AnyCast<tp>(a)))); \
        }
        WRITE_DATA_AS(uint8_t, uint32_t)
        WRITE_DATA_AS(int8_t, int32_t)
#undef WRITE_DATA_AS
        /// end of different numberic types
    else if (type == typeid(json::JsonNumber)) /// number literal
    {
        auto obj = AnyCast<json::JsonNumber>(a);
        ((JsonNumberWriter *)writer)->WriteJsonNumber(obj);
    }
    else                                            /// other un-supported type
    {
        AUTIL_LEGACY_THROW(
                ParameterInvalidException,
                std::string("See unsupported data in Any:") + type.name()
                           );
    }
}

}
}
