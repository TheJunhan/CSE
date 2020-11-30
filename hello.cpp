#include "hello.h"
#include <iostream>

using namespace std;

hello::hello(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    cout << "ok" << endl;
}