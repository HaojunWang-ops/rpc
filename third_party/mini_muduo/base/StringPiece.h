#pragma once

#include "Types.h"

#include <string.h>
#include <iosfwd>
#include <type_traits>

namespace reactor
{
    //
    class StringArg
    {
    public:
        StringArg(const char* str)
            : str_(str)
        {}
        StringArg(const string& str)
            : str_ (str.c_str())
        {}

        const char* c_str() const {return str_;}
    private:
        const char* str_;
    };

    //StringPiece不拥有数据，只是个轻量视图
    //必须保证StringPiece的生命周期短于底层那段string或char
    //不一定是null结尾
    class StringPiece
    {
    private:
        const char* ptr_;
        int length_;
    public:
        StringPiece()
            : ptr_(NULL), length_(0) {}
        StringPiece(const char* str)
            : ptr_ (str), length_ (static_cast<int> (strlen(ptr_))) {}
        StringPiece(const unsigned char* str)
            : ptr_ (reinterpret_cast<const char*> (str)),
              length_ (static_cast<int> (strlen(ptr_)))
        {}
        StringPiece(const string& str)
            : ptr_(str.data()),
              length_(static_cast<int> (str.size()))
        {}
        StringPiece(const char* offset, int len)
            : ptr_(offset), length_(len) {}

        
        const char* data() const {return ptr_;}
        int size() const {return length_;}
        bool empty() const {return length_ == 0;}
        const char* begin() const {return ptr_;}
        const char* end() const {return ptr_ + length_;}

        void clear() {ptr_ = nullptr; length_ = 0;}
        void set(const char* buf, int len) {ptr_ = buf; length_ = len;}
        void set(const char* str)
        {
            ptr_ = str;
            length_ = static_cast<int> (strlen(str));
        }
        void set(const void* buffer, int len)
        {
            ptr_ = reinterpret_cast<const char*> (buffer);
            length_ = len;
        }

        char operator[](int i) const {return ptr_[i];}

        void remove_prefix(int n)   {
            ptr_ += n;
            length_ -= n;
        }

        void remove_suffix(int n)
        {
            length_ -= n;
        }

        bool operator==(const StringPiece& x) const
        {
            return ((length_ == x.length_) && (memcmp(ptr_, x.ptr_, length_)));
        }
        bool operator!=(const StringPiece& x) const
        {
            return !(*this == x);
        }

        #define STRINGPIECE_BINARY_PREDICATE(cmp, auxcmp)                                   \
            bool operator cmp (const StringPiece& x) const{                                 \
                int r  = memcmp(ptr_, x.ptr_, length_ < x.length_ ? length_ : x.length_);   \
                return ((r auxcmp 0) || ((r == 0) && (length_ cmp x.length_)));             \
            }
            STRINGPIECE_BINARY_PREDICATE(<, <);
            STRINGPIECE_BINARY_PREDICATE(<=, <);
            STRINGPIECE_BINARY_PREDICATE(>=, >);
            STRINGPIECE_BINARY_PREDICATE(>, >);  
        #undef STRINGPIECE_BINARY_PREDICATE

        int compare(const StringPiece& x) const {
            int r = memcmp(ptr_, x.ptr_, length_ < x.length_ ? length_ : x.length_);
            if (r == 0)
            {
                if (length_ < x.length_) 
                {
                    r = -1;
                }
                else if(length_ > x.length_)
                {
                    r = +1;
                }
            }
            return r;
        }

        string as_string() const {
            return string(data(), size());
        }

        void CopyToString(string* target) const{
            target->assign(ptr_, length_);
        }

        bool starts_with(const StringPiece& x) const
        {
            return ((length_ >= x.length_) && (memcmp(ptr_, x.ptr_, x.length_)));
        }
    };
}

static_assert(std::is_trivially_copyable<reactor::StringPiece>::value, 
              "StringPiece should be trivially copyable for vector memmove optimization");

std::ostream& operator<<(std::ostream& o, const reactor::StringPiece& piece);