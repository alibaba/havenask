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
#ifndef _ATOMIC_H
#define _ATOMIC_H

/**
 ** Atomic type.
 **/
typedef struct {
    volatile int counter;
} atomic_t;

typedef struct {
    volatile long long counter;
} atomic64_t;

#ifndef ATOMIC_INIT
#define ATOMIC_INIT(i)                                                                                                 \
    { (i) }
#endif

/* Atomic operations. */
#define atomic_read(v) ((v)->counter)
#define atomic_set(v, i) (((v)->counter) = (i))

#define atomic_add(i, v) ((void)__sync_add_and_fetch(&(v)->counter, i))
#define atomic_sub(i, v) ((void)__sync_sub_and_fetch(&(v)->counter, i))
#define atomic_sub_and_test(i, v) ((__sync_sub_and_fetch(&(v)->counter, i)))
#define atomic_inc(v) ((void)__sync_fetch_and_add(&(v)->counter, 1))
#define atomic_dec(v) ((void)__sync_fetch_and_sub(&(v)->counter, 1))

#define atomic_add_return(i, v) (__sync_add_and_fetch(&(v)->counter, i))
#define atomic_sub_return(i, v) (__sync_sub_and_fetch(&(v)->counter, i))
#define atomic_inc_return(v) (__sync_add_and_fetch(&(v)->counter, 1))
#define atomic_dec_return(v) (__sync_sub_and_fetch(&(v)->counter, 1))

/* return true if the result is 0, or false otherwise. */
#define atomic_dec_and_test(v) (!(__sync_sub_and_fetch(&(v)->counter, 1)))
#define atomic_inc_and_test(v) (!(__sync_add_and_fetch(&(v)->counter, 1)))

/* general operations */
/* return true if swap is done or false otherwise */
#define compare_and_swap(v, oldval, newval) (__sync_bool_compare_and_swap(&(v)->counter, oldval, newval))

#endif
