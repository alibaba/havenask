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
// patch autil json to support null value parsing
#include "autil/legacy/jsonizable.h"

// Autil parse error when value is `null`, e.g: {key: null} . Use this patch to fix the issue.
namespace autil {
namespace legacy {
    template <typename T>
    struct Nullable {
        T value;

        T& operator* () {
            return value;
        }

        T& operator= (const T& other) {
            value = other;
            return value;
        }

        operator T& () {
            return value;
        }
    };
    template <typename T>
    inline void FromRapidValue(Nullable<T>& t, RapidValue& value) {
        if (!value.IsNull()) {
            FromRapidValue(t.value, value);
        }
    }

    template <typename T>
    inline void FromJson(Nullable<T>& t, const Any& a) {
        if (a.GetType() != typeid(void)) {
            FromJson(t.value, a);
        }
    }

    template <typename T>
    inline Any ToJson(const Nullable<T>& t) {
        return ToJson(t.value);
    }
}
}
