#pragma once

#include "Types.h"

#include <exception>

namespace reactor{

    class Exception : public std::exception
    {
    public:
        Exception(string what);
        ~Exception()noexcept override = default;

        //override 表示这个函数必须是用来重写父类的虚函数的
        //父类的虚函数必须写virtual
        //子类的重写函数必须写override，不要写virtual
        const char* what() const noexcept override
        {
            return message_.c_str();
        }

        const char* stackTrace() const noexcept
        {
            return stack_.c_str();
        }

    private:
        string message_;
        string stack_;
    };
}