module FetchUtils

using Base.Threads

export RateLimiter, throttle!, run_bounded_tasks

"""
    RateLimiter(interval; jitter=0.0)

Simple cross-thread rate limiter. Call `throttle!(limiter)` before issuing a request to enforce
at least `interval` seconds between calls, plus optional random jitter to spread bursts.
"""
mutable struct RateLimiter
    interval::Float64
    jitter::Float64
    last_time::Base.RefValue{Float64}
    lock::ReentrantLock
end

function RateLimiter(interval; jitter::Float64=0.0)
    RateLimiter(interval, jitter, Ref(0.0), ReentrantLock())
end

function throttle!(limiter::RateLimiter)
    lock(limiter.lock)
    try
        now = time()
        wait_for = limiter.last_time[] + limiter.interval + rand() * limiter.jitter - now
        if wait_for > 0
            sleep(wait_for)
        end
        limiter.last_time[] = time()
    finally
        unlock(limiter.lock)
    end
end

"""
    run_bounded_tasks(items; max_concurrency::Int=2, task_fn)

Run `task_fn(item)` over `items` with at most `max_concurrency` concurrent tasks.
Returns a vector of results aligned with the input order.
"""
function run_bounded_tasks(items; max_concurrency::Int=2, task_fn)
    n = length(items)
    results = Vector{Any}(undef, n)
    index_ref = Ref(1)
    index_lock = ReentrantLock()

    Threads.@sync begin
        for _ in 1:max(1, min(max_concurrency, n))
            Threads.@spawn begin
                while true
                    idx = 0
                    lock(index_lock)
                    if index_ref[] <= n
                        idx = index_ref[]
                        index_ref[] += 1
                    end
                    unlock(index_lock)
                    idx == 0 && break

                    results[idx] = task_fn(items[idx])
                end
            end
        end
    end

    return results
end

end # module
