#pragma once

#include "noncopyable.h"

#include <stdint.h>
#include <atomic>

namespace reactor
{
    namespace detail
    {
        template <typename T>
        class AtomicIntegerT : noncopyable
        {
        public:
            AtomicIntegerT()
                : value_(0)
            {
            }

            T get()
            {
                return value_.load();
            }

            T getAndAdd(T x)
            {
                return value_.fetch_add(x);
            }

            T addAndGet(T x)
            {
                return getAndAdd(x) + x;
            }

            T incrementAndGet()
            {
                return addAndGet(1);
            }

            T decrementAndGet()
            {
                return addAndGet(-1);
            }

            void add(T x)
            {
                getAndAdd(x);
            }

            void increment()
            {
                incrementAndGet();
            }

            void decrement()
            {
                decrementAndGet();
            }

            T getAndSet(T newvalue)
            {
                return value_.exchange(newvalue);
            }

        private:
            std::atomic<T> value_;
        };
    }
    typedef detail::AtomicIntegerT<int32_t> AtomicInt32;
    typedef detail::AtomicIntegerT<int64_t> AtomicInt64;
}