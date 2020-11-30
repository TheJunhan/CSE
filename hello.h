#include <string>
#include "extent_client.h"

class hello
{
    private:
    extent_client *ec;
    public:
    hello(std::string extent_dst);
};