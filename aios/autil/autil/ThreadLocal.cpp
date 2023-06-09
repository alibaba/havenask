//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "autil/ThreadLocal.h"

#include <assert.h>
#include <stdlib.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"

using namespace std;
using namespace autil;

namespace autil {

__thread ThreadLocalPtr::ThreadData* ThreadLocalPtr::StaticMeta::tls_ = nullptr;

void ThreadLocalPtr::InitSingletons() { ThreadLocalPtr::Instance(); }

ThreadLocalPtr::StaticMeta* ThreadLocalPtr::Instance() {
  // Here we prefer function static variable instead of global
  // static variable as function static variable is initialized
  // when the function is first call.  As a result, we can properly
  // control their construction order by properly preparing their
  // first function call.
  //
  // Note that here we decide to make "inst" a static pointer w/o deleting
  // it at the end instead of a static variable.  This is to avoid the following
  // destruction order desester happens when a child thread using ThreadLocalPtr
  // dies AFTER the main thread dies:  When a child thread happens to use
  // ThreadLocalPtr, it will try to delete its thread-local data on its
  // OnThreadExit when the child thread dies.  However, OnThreadExit depends
  // on the following variable.  As a result, if the main thread dies before any
  // child thread happen to use ThreadLocalPtr dies, then the destruction of
  // the following variable will go first, then OnThreadExit, therefore causing
  // invalid access.
  //
  // The above problem can be solved by using thread_local to store tls_ instead
  // of using __thread.  The major difference between thread_local and __thread
  // is that thread_local supports dynamic construction and destruction of
  // non-primitive typed variables.  As a result, we can guarantee the
  // destruction order even when the main thread dies before any child threads.
  // However, thread_local is not supported in all compilers that accept -std=c++11
  // (e.g., eg Mac with XCode < 8. XCode 8+ supports thread_local).
  static ThreadLocalPtr::StaticMeta* inst = new ThreadLocalPtr::StaticMeta();
  return inst;
}

autil::ThreadMutex& ThreadLocalPtr::StaticMeta::Mutex() { return Instance()->mutex_; }

void ThreadLocalPtr::StaticMeta::OnThreadExit(void* ptr) {
  auto* tls = static_cast<ThreadData*>(ptr);
  assert(tls != nullptr);

  // Use the cached StaticMeta::Instance() instead of directly calling
  // the variable inside StaticMeta::Instance() might already go out of
  // scope here in case this OnThreadExit is called after the main thread
  // dies.
  auto* inst = tls->inst;
  pthread_setspecific(inst->pthread_key_, nullptr);

  ScopedLock l(inst->MemberMutex());
  inst->RemoveThreadData(tls);
  // Unref stored pointers of current thread from all instances
  uint32_t id = 0;
  for (auto& e : tls->entries) {
    void* raw = e.ptr.load();
    if (raw != nullptr) {
      auto unref = inst->GetHandler(id);
      if (unref != nullptr) {
        unref(raw);
      }
    }
    ++id;
  }
  // Delete thread local structure no matter if it is Mac platform
  delete tls;
}

ThreadLocalPtr::StaticMeta::StaticMeta() : next_instance_id_(0), head_(this) {
  if (pthread_key_create(&pthread_key_, &OnThreadExit) != 0) {
    abort();
  }

  // OnThreadExit is not getting called on the main thread.
  // Call through the static destructor mechanism to avoid memory leak.
  //
  // Caveats: ~A() will be invoked _after_ ~StaticMeta for the global
  // singleton (destructors are invoked in reverse order of constructor
  // _completion_); the latter must not mutate internal members. This
  // cleanup mechanism inherently relies on use-after-release of the
  // StaticMeta, and is brittle with respect to compiler-specific handling
  // of memory backing destructed statically-scoped objects. Perhaps
  // registering with atexit(3) would be more robust.
  //
  static struct A {
    ~A() {

      if (tls_) {
        OnThreadExit(tls_);
      }
    }
  } a;

  head_.next = &head_;
  head_.prev = &head_;
}

void ThreadLocalPtr::StaticMeta::AddThreadData(ThreadLocalPtr::ThreadData* d) {
//  Mutex()->AssertHeld();
  d->next = &head_;
  d->prev = head_.prev;
  head_.prev->next = d;
  head_.prev = d;
}

void ThreadLocalPtr::StaticMeta::RemoveThreadData(
    ThreadLocalPtr::ThreadData* d) {
  // Mutex()->AssertHeld();
  d->next->prev = d->prev;
  d->prev->next = d->next;
  d->next = d->prev = d;
}

ThreadLocalPtr::ThreadData* ThreadLocalPtr::StaticMeta::GetThreadLocal() {
  if (unlikely(tls_ == nullptr)) {
    auto* inst = Instance();
    tls_ = new ThreadData(inst);
    {
      // Register it in the global chain, needs to be done before thread exit
      // handler registration
      ScopedLock l(Mutex());
      inst->AddThreadData(tls_);
    }
    // Even it is not OS_MACOSX, need to register value for pthread_key_ so that
    // its exit handler will be triggered.
    if (pthread_setspecific(inst->pthread_key_, tls_) != 0) {
      {
        ScopedLock l(Mutex());        
        inst->RemoveThreadData(tls_);
      }
      delete tls_;
      abort();
    }
  }
  return tls_;
}

void* ThreadLocalPtr::StaticMeta::Get(uint32_t id) const {
  auto* tls = GetThreadLocal();
  if (unlikely(id >= tls->entries.size())) {
    return nullptr;
  }
  return tls->entries[id].ptr.load(std::memory_order_acquire);
}

void ThreadLocalPtr::StaticMeta::Reset(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (unlikely(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    ScopedLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  tls->entries[id].ptr.store(ptr, std::memory_order_release);
}

void* ThreadLocalPtr::StaticMeta::Swap(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (unlikely(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    ScopedLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  return tls->entries[id].ptr.exchange(ptr, std::memory_order_acquire);
}

bool ThreadLocalPtr::StaticMeta::CompareAndSwap(uint32_t id, void* ptr,
    void*& expected) {
  auto* tls = GetThreadLocal();
  if (unlikely(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    ScopedLock l(Mutex()); 
    tls->entries.resize(id + 1);
  }
  return tls->entries[id].ptr.compare_exchange_strong(
      expected, ptr, std::memory_order_release, std::memory_order_relaxed);
}

void ThreadLocalPtr::StaticMeta::Scrape(uint32_t id, autovector<void*>* ptrs,
    void* const replacement) {
  ScopedLock l(Mutex()); 
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr =
          t->entries[id].ptr.exchange(replacement, std::memory_order_acquire);
      if (ptr != nullptr) {
        ptrs->push_back(ptr);
      }
    }
  }
}

void ThreadLocalPtr::StaticMeta::Fold(uint32_t id, FoldFunc func, void* res) {
  ScopedLock l(Mutex()); 
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.load();
      if (ptr != nullptr) {
        func(ptr, res);
      }
    }
  }
}

void ThreadLocalPtr::StaticMeta::SetHandler(uint32_t id, UnrefHandler handler) {
  ScopedLock l(Mutex());  
  handler_map_[id] = handler;
}

UnrefHandler ThreadLocalPtr::StaticMeta::GetHandler(uint32_t id) {
  // Mutex()->AssertHeld();
  auto iter = handler_map_.find(id);
  if (iter == handler_map_.end()) {
    return nullptr;
  }
  return iter->second;
}

uint32_t ThreadLocalPtr::StaticMeta::GetId() {
  ScopedLock l(Mutex());
  if (free_instance_ids_.empty()) {
    return next_instance_id_++;
  }

  uint32_t id = free_instance_ids_.back();
  free_instance_ids_.pop_back();
  return id;
}

uint32_t ThreadLocalPtr::StaticMeta::PeekId() const {
  ScopedLock l(Mutex());
  if (!free_instance_ids_.empty()) {
    return free_instance_ids_.back();
  }
  return next_instance_id_;
}

void ThreadLocalPtr::StaticMeta::ReclaimId(uint32_t id) {
  // This id is not used, go through all thread local data and release
  // corresponding value
  ScopedLock l(Mutex());
  auto unref = GetHandler(id);
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.exchange(nullptr);
      if (ptr != nullptr && unref != nullptr) {
        unref(ptr);
      }
    }
  }
  handler_map_[id] = nullptr;
  free_instance_ids_.push_back(id);
}

ThreadLocalPtr::ThreadLocalPtr(UnrefHandler handler)
    : id_(Instance()->GetId()) {
  if (handler != nullptr) {
    Instance()->SetHandler(id_, handler);
  }
}

ThreadLocalPtr::~ThreadLocalPtr() {
  Instance()->ReclaimId(id_);
}

void* ThreadLocalPtr::Get() const {
  return Instance()->Get(id_);
}

void ThreadLocalPtr::Reset(void* ptr) {
  Instance()->Reset(id_, ptr);
}

void* ThreadLocalPtr::Swap(void* ptr) {
  return Instance()->Swap(id_, ptr);
}

bool ThreadLocalPtr::CompareAndSwap(void* ptr, void*& expected) {
  return Instance()->CompareAndSwap(id_, ptr, expected);
}

void ThreadLocalPtr::Scrape(autovector<void*>* ptrs, void* const replacement) {
  Instance()->Scrape(id_, ptrs, replacement);
}

void ThreadLocalPtr::Fold(FoldFunc func, void* res) {
  Instance()->Fold(id_, func, res);
}

}
