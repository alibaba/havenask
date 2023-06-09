/*********************************************************************
 * $Author: santai.ww $
 *
 * $LastChangedBy: santai.ww $
 *
 * $Revision: 8316 $
 *
 * $LastChangedDate: 2011-11-30 16:22:16 +0800 (Wed, 30 Nov 2011) $
 *
 * $Id: atomic.h 8316 2011-11-30 08:22:16Z santai.ww $
 *
 * $Brief: 数据原子操作类 $
 ********************************************************************/

/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#pragma once


namespace isearch {
namespace util {


/*
 * Atomic operations that C can't guarantee us.  Useful for
 * resource counting etc..
 */

#define LOCK "lock ; "

/*
 * Make sure gcc doesn't try to be clever and move things around
 * on us. We need to use _exactly_ the address the user gave us,
 * not some alias that contains the same information.
 */
typedef struct { volatile unsigned int counter; } atomic32_t;

#ifndef ATOMIC_INIT
#define ATOMIC_INIT(i)	{ (i) }
#endif
/**
 * atomic32_read - read atomic32 variable
 * @param v pointer of type atomic32_t
 *
 * Atomically reads the value of v.
 */
#define atomic32_read(v)		((v)->counter)

/**
 * atomic32_set - set atomic32 variable
 * @param v pointer of type atomic32_t
 * @param i required value
 *
 * Atomically sets the value of v to i.
 */
#define atomic32_set(v,i)		(((v)->counter) = (i))

/**
 * atomic32_add - add integer to atomic32 variable
 * @param i integer value to add
 * @param v pointer of type atomic32_t
 *
 * Atomically adds i to v.
 */
static __inline__ void atomic32_add(unsigned int i, atomic32_t *v)
{
	__asm__ __volatile__(
		LOCK "addl %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}

/**
 * atomic32_sub - subtract the atomic32 variable
 * @param i integer value to subtract
 * @param v pointer of type atomic32_t
 *
 * Atomically subtracts i from v.
 */
static __inline__ void atomic32_sub(unsigned int i, atomic32_t *v)
{
	__asm__ __volatile__(
		LOCK "subl %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}


/**
 * atomic32_add_return - add and return
 * @param v pointer of type atomic32_t
 * @param i integer value to add
 *
 * Atomically adds i to v and returns i + v
 */
static __inline__ unsigned int atomic32_add_return(unsigned int i, atomic32_t *v)
{
  unsigned int __i;
  /* Modern 486+ processor */
  __i = i;
  __asm__ __volatile__(
                LOCK "xaddl %0, %1"
                :"+r" (i), "+m" (v->counter)
                : : "memory");
  return i + __i;
}

static __inline__ unsigned int atomic32_sub_return(unsigned int i, atomic32_t *v)
{
  return atomic32_add_return(-i,v);
}


/**
 * atomic32_inc - increment atomic32 variable
 * @param v pointer of type atomic32_t
 *
 * Atomically increments v by 1.
 */
static __inline__ void atomic32_inc(atomic32_t *v)
{
	__asm__ __volatile__(
		LOCK "incl %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

/**
 * atomic32_dec - decrement atomic32 variable
 * @param v pointer of type atomic32_t
 *
 * Atomically decrements v by 1.
 */
static __inline__ void atomic32_dec(atomic32_t *v)
{
	__asm__ __volatile__(
		LOCK "decl %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

#define atomic32_inc_return(v)  (atomic32_add_return(1,v))
#define atomic32_dec_return(v)  (atomic32_sub_return(1,v))


typedef struct { volatile unsigned short counter; } atomic16_t;

#define ATOMIC16_INIT(i)	{ (i) }

/**
 * atomic16_read - read atomic16 variable
 * @param v pointer of type atomic16_t
 *
 * Atomically reads the value of v.
 */
#define atomic16_read(v)		((v)->counter)

/**
 * atomic16_set - set atomic16 variable
 * @param v pointer of type atomic16_t
 * @param i required value
 *
 * Atomically sets the value of v to i.
 */
#define atomic16_set(v,i)		(((v)->counter) = (i))

/**
 * atomic16_add - add integer to atomic16 variable
 * @param i integer value to add
 * @param v pointer of type atomic16_t
 *
 * Atomically adds i to v.
 */
static __inline__ void atomic16_add(unsigned short i, atomic16_t *v)
{
	__asm__ __volatile__(
		LOCK "addw %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}

/**
 * atomic16_sub - subtract the atomic16 variable
 * @param i integer value to subtract
 * @param v pointer of type atomic16_t
 *
 * Atomically subtracts i from v.
 */
static __inline__ void atomic16_sub(unsigned short i, atomic16_t *v)
{
	__asm__ __volatile__(
		LOCK "subw %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}


/**
 * atomic16_add_return - add and return
 * @param v pointer of type atomic16_t
 * @param i integer value to add
 *
 * Atomically adds i to v and returns i + v
 */
static __inline__ unsigned short atomic16_add_return(unsigned short i, atomic16_t *v)
{
  unsigned short __i;
  /* Modern 486+ processor */
  __i = i;
  __asm__ __volatile__(
                LOCK "xaddw %0, %1"
                :"+r" (i), "+m" (v->counter)
                : : "memory");
  return i + __i;
}

static __inline__ unsigned short atomic16_sub_return(unsigned short i, atomic16_t *v)
{
  return atomic16_add_return(-i,v);
}

/**
 * atomic16_inc - increment atomic16 variable
 * @param v pointer of type atomic16_t
 *
 * Atomically increments v by 1.
 */
static __inline__ void atomic16_inc(atomic16_t *v)
{
	__asm__ __volatile__(
		LOCK "incw %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

/**
 * atomic16_dec - decrement atomic16 variable
 * @param v pointer of type atomic16_t
 *
 * Atomically decrements v by 1.
 */
static __inline__ void atomic16_dec(atomic16_t *v)
{
	__asm__ __volatile__(
		LOCK "decw %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

#define atomic16_inc_return(v)  (atomic16_add_return(1,v))
#define atomic16_dec_return(v)  (atomic16_sub_return(1,v))

typedef struct { volatile long counter; } atomic64_t;

#define ATOMIC64_INIT(i)	{ (i) }

/**
 * atomic64_read - read atomic64 variable
 * @param v pointer of type atomic64_t
 *
 * Atomically reads the value of v.
 */
#define atomic64_read(v)		((v)->counter)

/**
 * atomic64_set - set atomic64 variable
 * @param v pointer of type atomic64_t
 * @param i required value
 *
 * Atomically sets the value of v to i.
 */
#define atomic64_set(v,i)		(((v)->counter) = (i))

/**
 * atomic64_add - add integer to atomic64 variable
 * @param i integer value to add
 * @param v pointer of type atomic64_t
 *
 * Atomically adds i to v.
 */
static __inline__ void atomic64_add(long i, atomic64_t *v)
{
	__asm__ __volatile__(
		LOCK "addq %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}

/**
 * atomic64_sub - subtract the atomic64 variable
 * @param i integer value to subtract
 * @param v pointer of type atomic64_t
 *
 * Atomically subtracts i from v.
 */
static __inline__ void atomic64_sub(long i, atomic64_t *v)
{
	__asm__ __volatile__(
		LOCK "subq %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}


/**
 * atomic64_add_return - add and return
 * @param v pointer of type atomic64_t
 * @param i integer value to add
 *
 * Atomically adds i to v and returns i + v
 */
static __inline__ long atomic64_add_return(long i, atomic64_t *v)
{
  long __i;
  /* Modern 486+ processor */
  __i = i;
  __asm__ __volatile__(
                LOCK "xaddq %0, %1"
                :"+r" (i), "+m" (v->counter)
                : : "memory");
  return i + __i;
}

static __inline__ long atomic64_sub_return(long i, atomic64_t *v)
{
  return atomic64_add_return(-i,v);
}

/**
 * atomic64_inc - increment atomic64 variable
 * @param v pointer of type atomic64_t
 *
 * Atomically increments v by 1.
 */
static __inline__ void atomic64_inc(atomic64_t *v)
{
	__asm__ __volatile__(
		LOCK "incq %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

/**
 * atomic64_dec - decrement atomic64 variable
 * @param v pointer of type atomic64_t
 *
 * Atomically decrements v by 1.
 */
static __inline__ void atomic64_dec(atomic64_t *v)
{
	__asm__ __volatile__(
		LOCK "decq %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

#define atomic64_inc_return(v)  (atomic64_add_return(1,v))
#define atomic64_dec_return(v)  (atomic64_sub_return(1,v))

typedef struct { volatile unsigned char counter; } atomic8_t;

#define atomic8_INIT(i)	{ (i) }

/**
 * atomic8_read - read atomic8 variable
 * @param v pointer of type atomic8_t
 *
 * Atomically reads the value of v.
 */
#define atomic8_read(v)		((v)->counter)

/**
 * atomic8_set - set atomic8 variable
 * @param v pointer of type atomic8_t
 * @param i required value
 *
 * Atomically sets the value of v to i.
 */
#define atomic8_set(v,i)		(((v)->counter) = (i))

/**
 * atomic8_add - add integer to atomic8 variable
 * @param i integer value to add
 * @param v pointer of type atomic8_t
 *
 * Atomically adds i to v.
 */
static __inline__ void atomic8_add(unsigned char i, atomic8_t *v)
{
	__asm__ __volatile__(
		LOCK "addb %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}

/**
 * atomic8_sub - subtract the atomic8 variable
 * @param i integer value to subtract
 * @param v pointer of type atomic8_t
 *
 * Atomically subtracts i from v.
 */
static __inline__ void atomic8_sub(unsigned char i, atomic8_t *v)
{
	__asm__ __volatile__(
		LOCK "subb %1,%0"
		:"=m" (v->counter)
		:"ir" (i), "m" (v->counter));
}


/**
 * atomic8_add_return - add and return
 * @param v pointer of type atomic8_t
 * @param i integer value to add
 *
 * Atomically adds i to v and returns i + v
 */
static __inline__ unsigned char atomic8_add_return(unsigned char i, atomic8_t *v)
{
  unsigned char __i;
  /* Modern 486+ processor */
  __i = i;
  __asm__ __volatile__(
                LOCK "xaddb %0, %1"
                :"+r" (i), "+m" (v->counter)
                : : "memory");
  return i + __i;
}

static __inline__ unsigned char atomic8_sub_return(unsigned char i, atomic8_t *v)
{
  return atomic8_add_return(-i,v);
}

/**
 * atomic8_inc - increment atomic8 variable
 * @param v pointer of type atomic8_t
 *
 * Atomically increments v by 1.
 */
static __inline__ void atomic8_inc(atomic8_t *v)
{
	__asm__ __volatile__(
		LOCK "incb %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

/**
 * atomic8_dec - decrement atomic8 variable
 * @param v pointer of type atomic8_t
 *
 * Atomically decrements v by 1.
 */
static __inline__ void atomic8_dec(atomic8_t *v)
{
	__asm__ __volatile__(
		LOCK "decb %0"
		:"=m" (v->counter)
		:"m" (v->counter));
}

#define atomic8_inc_return(v)  (atomic8_add_return(1,v))
#define atomic8_dec_return(v)  (atomic8_sub_return(1,v))

} // namespace util
} // namespace isearch
