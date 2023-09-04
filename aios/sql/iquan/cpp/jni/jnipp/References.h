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

#include "Exceptions.h"
#include "References-fwd.h"
#include "TypeTraits.h"

#ifdef JNIPP_REFERENCE_LOGGING
#include <iostream>
#else
#define reference_logging(...)
#endif

namespace iquan {

template <typename T, typename Alloc>
class StrongRef {
public:
    using ctype = CType<T>;
    using jtype = JType<T>;

#ifdef JNIPP_REFERENCE_LOGGING
    StrongRef() noexcept {
        reference_logging("ctor", nullptr);
    }
#else
    StrongRef() noexcept = default;
#endif

    // takeover o
    StrongRef(jtype o) noexcept {
        reference_logging("ctor with plain", o);
        _cobj.set(o);
    }

    StrongRef(const StrongRef &other) {
        reference_logging("copy ctor", other.get());
        auto o = Alloc {}.ref(other.get());
        _cobj.set(static_cast<jtype>(o));
    }

    template <typename U, typename OtherAlloc>
    StrongRef(const StrongRef<U, OtherAlloc> &other) {
        static_assert(std::is_base_of<CType<T>, CType<U>>::value,
                      "Ref holder jni type not compatible");
        reference_logging("copy ctor", other.get());
        auto o = Alloc {}.ref(other.get());
        _cobj.set(static_cast<jtype>(o));
    }

    StrongRef(StrongRef &&other) {
        reference_logging("move ctor", other.get());
        _cobj.set(other.release());
    }

    template <typename U>
    StrongRef(StrongRef<U, Alloc> &&other) {
        static_assert(std::is_base_of<CType<T>, CType<U>>::value,
                      "Ref holder jni type not compatible");
        reference_logging("move ctor", other.get());
        _cobj.set(static_cast<jtype>(other.release()));
    }

    // There is no move ctor from OtherAlloc

    template <typename U>
    StrongRef(const AliasRef<U> &other) {
        static_assert(std::is_base_of<CType<T>, CType<U>>::value,
                      "Ref holder jni type not compatible");
        reference_logging("copy ctor from alias", other.get());
        auto o = Alloc {}.ref(other.get());
        _cobj.set(static_cast<jtype>(o));
    }

    StrongRef &operator=(const StrongRef &other) {
        reference_logging("assign from", other.get());
        if (this != &other) {
            auto o = Alloc {}.ref(other.get());
            set(static_cast<jtype>(o));
        }
        return *this;
    }

    template <typename U, typename OtherAlloc>
    StrongRef &operator=(const StrongRef<U, OtherAlloc> &other) {
        static_assert(std::is_base_of<CType<T>, CType<U>>::value,
                      "Ref holder jni type not compatible");
        reference_logging("assign from", other.get());
        auto o = Alloc {}.ref(other.get());
        set(static_cast<jtype>(o));

        return *this;
    }

    StrongRef &operator=(StrongRef &&other) {
        reference_logging("assign move", other.get());
        if (this != &other) {
            set(static_cast<jtype>(other.release()));
        }
        return *this;
    }

    template <typename U>
    StrongRef &operator=(StrongRef<U, Alloc> &&other) {
        static_assert(std::is_base_of<CType<T>, CType<U>>::value,
                      "Ref holder jni type not compatible");
        reference_logging("assign move", other.get());
        set(static_cast<jtype>(other.release()));
        return *this;
    }

    // There is no assign move from OtherAlloc

    template <typename U>
    StrongRef &operator=(const AliasRef<U> &other) {
        static_assert(std::is_base_of<CType<T>, CType<U>>::value,
                      "Ref holder jni type not compatible");
        reference_logging("assign from alias", other.get());
        auto o = Alloc {}.ref(other.get());
        set(static_cast<jtype>(o));

        return *this;
    }

    ~StrongRef() {
        reference_logging("dtor", nullptr);
        set(nullptr);
    }

    // simply get
    jtype get() const noexcept {
        return _cobj.get();
    }
    // handover the underlying ref
    jtype release() noexcept {
        auto ref = get();
        _cobj.set(nullptr);
        return ref;
    }
    // unref old if has, then take the new one
    void set(jtype o) noexcept {
        if (get()) {
            Alloc {}.unref(get());
        }
        _cobj.set(o);
    }
    // return old, no ref or unref happened
    jtype swap(jtype o) noexcept {
        jtype old = get();
        _cobj.set(o);
        return old;
    }
    // set to null, unref prev if not null
    void reset() noexcept {
        set(nullptr);
    }

    ctype *operator->() noexcept {
        return &_cobj;
    }
    const ctype *operator->() const noexcept {
        return &_cobj;
    }
    ctype &operator*() noexcept {
        return _cobj;
    }
    const ctype &operator*() const noexcept {
        return _cobj;
    }

#ifdef JNIPP_REFERENCE_LOGGING
    void reference_logging(const std::string &name, jtype o) {
        std::cout << Alloc::name << " ref " << name << ": " << static_cast<void *>(get()) << " <- "
                  << static_cast<void *>(o) << std::endl;
    }
#endif

private:
    ctype _cobj;
};

template <typename T>
class AliasRef {
public:
    using ctype = CType<T>;
    using jtype = JType<T>;
    using _jtype = typename std::remove_pointer<jtype>::type;
#ifdef JNIPP_REFERENCE_LOGGING
    AliasRef() noexcept {
        reference_logging("ctor", nullptr);
    }
    ~AliasRef() noexcept {
        reference_logging("dtor", nullptr);
    }
#else
    constexpr AliasRef() noexcept = default;
    ~AliasRef() noexcept = default;
#endif

    AliasRef(jtype o) noexcept {
        reference_logging("ctor with plain", o);
        set(o);
    }

    template <typename ST>
    AliasRef(const AliasRef<ST> &other) noexcept {
        reference_logging("copy ctor", other.get());
        set(other.get());
    }

    template <typename ST, typename Alloc>
    AliasRef(const StrongRef<ST, Alloc> &other) {
        reference_logging("copy ctor from strong", other.get());
        set(other.get());
    }

    AliasRef &operator=(const AliasRef &other) {
        reference_logging("copy ctor", other.get());
        if (this != &other) {
            set(other.get());
        }
        return *this;
    }

    template <typename ST>
    AliasRef &operator=(const AliasRef<ST> &other) {
        reference_logging("assign from", other.get());
        if ((void *)this != (void *)&other) {
            set(other.get());
        }
        return *this;
    }

    template <typename ST, typename Alloc>
    AliasRef &operator=(const StrongRef<ST, Alloc> &other) {
        reference_logging("assign from strong", other.get());
        set(other.get());
        return *this;
    }

    jtype get() const noexcept {
        return _cobj.get();
    }
    void set(jtype o) noexcept {
        _cobj.set(o);
    }

    ctype *operator->() noexcept {
        return &_cobj;
    }
    const ctype *operator->() const noexcept {
        return &_cobj;
    }
    ctype &operator*() noexcept {
        return _cobj;
    }
    const ctype &operator*() const noexcept {
        return _cobj;
    }

private:
#ifdef JNIPP_REFERENCE_LOGGING
    void reference_logging(const std::string &name, jtype o) {
        std::cout << "alias ref " << name << ": " << static_cast<void *>(get()) << " <- "
                  << static_cast<void *>(o) << std::endl;
    }
#endif

    ctype _cobj;
};

} // namespace iquan
