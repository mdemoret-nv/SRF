#include <rxcpp/operators/rx-observe_on.hpp>

#include <functional>
#include <thread>

namespace srf::runnable {

std::thread srf_thread_factory(std::function<void()> task);

rxcpp::observe_on_one_worker observe_on_new_srf_thread()
{
    static rxcpp::observe_on_one_worker r(rxcpp::rxsc::make_new_thread(&srf::runnable::srf_thread_factory));
    return r;
}

}  // namespace srf::runnable
