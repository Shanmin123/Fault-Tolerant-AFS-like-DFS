import math


def is_prime(n: int) -> bool:
    if n < 2:
        return False
        # use some small primes for preliminary quick check
    example_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37]
    if n in example_primes:
        return True
    if any(n % p == 0 for p in example_primes):
        return False
    # use Miller_Robin theorem to test prime
    d, s = n - 1, 0
    while d % 2 == 0:
        d //= 2
        s += 1
    # test_base, these 7 bases can check for 64-bit integers.
    # source: by Jim Sinclair, https://miller-rabin.appspot.com/
    test_base = [2, 325, 9375, 28178, 450775, 9780504, 1795265022]
    for a in test_base:
        if n % a == 0:
            continue
        x = pow(a, d, n)
        if x == n - 1 or x == 1:
            continue
        for _ in range(s - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True
    #
    # if n < 2:
    #     return False
    # if n == 2:
    #     return True
    # if n % 2 == 0:
    #     return False
    # for i in range(3, int(math.isqrt(n)) + 1, 2):
    #     if n % i == 0:
    #         return False
    # return True